import pandas as pd
import re
import logging
import config
from google.cloud import firestore

db = firestore.Client()
logging.basicConfig(level=logging.INFO)


def data_from_store(bucket_name, blob_name):
    path = 'gs://{}/{}'.format(bucket_name, blob_name)
    logging.info('Reading {}'.format(path))
    new_data = pd.ExcelFile(path)
    logging.info('Read file {} from {}'.format(blob_name, bucket_name))
    return new_data


def extract_project_info_rapportage(df):
    df_voorblad = df.parse('Voorblad')
    re_digits = r'\d{6}'
    re_date = r'\d{4}-\d{2}'
    project_nr = re.findall(re_digits, str(df_voorblad.iloc[7, 2]))
    project_name = str(df_voorblad.iloc[8, 2])
    date = re.findall(re_date, str(df_voorblad.iloc[12, 2]))
    if project_nr and project_name:
        return project_name, project_nr[0], date[0]
    else:
        raise Exception(
            f'No name, date and project number found in the "voorblad": {project_name} {project_nr} - {date}'
        )


def extract_data_rapportage(df):
    df_detail = df.parse('Detail')
    df_detail = df_detail.iloc[20:, :]
    df_detail.iloc[0] = df_detail.iloc[0, :].ffill().astype(str)
    df_detail.columns = df_detail.iloc[0, :]
    df_detail = df_detail.iloc[1:, :]
    df_detail = df_detail[['ONDERAANNEMING', 'Prognose einde werk']]
    cols = [
        'artikelcode',
        'omschrijving',
        'nan',
        'prognose_einde_werk_aantallen',
        'prognose_einde_werk_tarief',
        'prognose_einde_werk_bedrag',
        'nan'
    ]
    df_detail.columns = cols
    df_detail = df_detail[df_detail.omschrijving.notna()]
    df_detail = df_detail[df_detail.artikelcode.notna()]
    df_detail.drop(columns=['nan'], inplace=True)
    logging.info('Extracted "Prognose einde werk"')
    return df_detail


def transform_rapportage(df, project_name, project_nr, date):
    logging.info('Start transforming data ...')
    df.artikelcode = df.artikelcode.astype(str).str.strip()
    prognose = df[['artikelcode',
                   'omschrijving',
                   'prognose_einde_werk_bedrag']].rename(columns={'prognose_einde_werk_bedrag': 'kostenbedrag'})
    prognose['project'] = project_nr
    prognose['date'] = date
    prognose['project_name'] = project_name
    prognose = prognose[prognose.kostenbedrag > 0]
    logging.info('Transforming data finsihed')
    return prognose


def transform_baan(df, column_mapping):
    df.rename(columns=column_mapping, inplace=True)
    df = df[column_mapping.values()]
    df.project = df.project.astype(str)
    df.artikelcode = df.artikelcode.astype(str).str.strip()
    return df


def aggregate_artikelcodes(df):
    return df.groupby('artikelcode').agg({'project': 'first',
                                          'kostenbedrag': 'sum',
                                          'categorie': 'first',
                                          'sub_categorie': 'first'}).reset_index().round(2)


def add_categorisering(df):
    categorisering = db.collection('Finance').document('FttX_Article_Categorization').get()
    categorisering = pd.DataFrame(categorisering.to_dict().get('record'))
    categorisering = categorisering[['artikelcode', 'categorie', 'sub_categorie']]
    df = df.merge(categorisering, on='artikelcode', how='left')
    df.sub_categorie.fillna('no_sub_category', inplace=True)
    df.categorie.fillna('no_category', inplace=True)
    logging.info('Add (sub)categories')
    return df


def update_firestore(df, project_nr, project_name, client, document_name):
    logging.info(f'Start updating "{document_name}" {project_name} ...')
    doc = db.collection('Finance').document(project_name).get()
    if doc.exists:
        record_string = 'record' + '.' + document_name
        db.collection('Finance').document(doc.id).update({record_string: df.to_dict(orient='records')})
    else:
        document = dict(client=client,
                        project_nr=project_nr,
                        project=project_name,
                        record={document_name: df.to_dict(orient='records')})
        db.collection('Finance').document(document.get('project')).set(document)


def process_project_rapportage(df):
    logging.info('Start processing file PROJECTRAPPORTAGE')
    _, project_nr, date = extract_project_info_rapportage(df)
    relevant = False
    for item in config.FINANCE_PROJECT_LIST.values():
        if item.get('project_nr') == project_nr:
            project_name = item.get('project_name')
            client = item.get('client')
            relevant = True
            break

    if relevant:
        logging.info(f'Start extracting projectrapportage: {project_name} {project_nr} - {date}')
        df = extract_data_rapportage(df)
        df = transform_rapportage(df, project_name, project_nr, date)
        df = add_categorisering(df)
        if not df.empty:
            update_firestore(df, project_nr, project_name, client, 'expected_actuals')
        else:
            logging.info('No records found in the excel file')
            return
    else:
        logging.info(f'This project is not in scope of the financial analysis: {project_nr}')
        return
    logging.info('Processing file PROJECTRAPPORTAGE finished')
    return df


def update_baan_realisatie(df):
    df = add_categorisering(df)
    for k, v in config.FINANCE_PROJECT_LIST.items():
        info = v
        project_nr = info.get('project_nr')
        actuals = df[df.project == project_nr]
        actuals_aggregated = aggregate_artikelcodes(actuals)
        project_name = info.get('project_name')
        client = info.get('client')
        if not actuals.empty:
            update_firestore(actuals, project_nr, project_name, client, 'actuals')
        if not actuals_aggregated.empty:
            update_firestore(actuals_aggregated, project_nr, project_name, client, 'actuals_aggregated')


def update_baan_budget(df):
    df = add_categorisering(df)
    for k, v in config.FINANCE_PROJECT_LIST.items():
        info = v
        project_nr = info.get('project_nr')
        project_name = info.get('project_name')
        client = info.get('client')
        budget = aggregate_artikelcodes(df[df.project == project_nr])
        if not budget.empty:
            update_firestore(budget, project_nr, project_name, client, 'budget')


def process_baan(df):
    df_realisatie = transform_baan(df.parse(config.BAAN_SHEET_REALISATION), config.BAAN_REALISATION_MAPPING)
    df_budget = transform_baan(df.parse(config.BAAN_SHEET_BUDGET), config.BAAN_BUDGET_MAPPING)
    update_baan_realisatie(df_realisatie)
    update_baan_budget(df_budget)


def handler(data, context):
    logging.info('Run started')
    bucket = data['bucket']
    filename = data['name']
    try:
        if 'uploads/project_rapportage/' in filename:
            df = data_from_store(bucket, filename)
            process_project_rapportage(df)
            logging.info('Run finished')
            return 'OK', 200
        elif 'uploads/baan/' in filename:
            df = data_from_store(bucket, filename)
            process_baan(df)
            logging.info('Run finished')
            return 'OK', 200
        else:
            logging.info(f'Skipping {filename} due because it does not need processing')
            logging.info('Run finished')
            return 'OK', 200
    except Exception as e:
        logging.error(f'Processing {filename} stopped')
        logging.error(f'Processing failure: {e}')
        return 'Error', 500
