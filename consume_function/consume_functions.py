import itertools
import json
import base64
from datetime import datetime
import logging
from google.cloud import firestore, secretmanager
from pandas._libs.tslibs.nattype import NaTType
from sqlalchemy.engine import ResultProxy

import config
from sqlalchemy import create_engine
from contextlib import contextmanager
import pandas as pd
import numpy as np

db = firestore.Client()


def process_fiberconnect(records, topic_config):
    logging.info("Processing fiber connect")
    records, logs = prepare_records(records)
    write_logs_to_sql(logs)
    write_records_to_sql(records=records,
                         topic_config=topic_config)


def parse_request(request):
    envelope = json.loads(request.data.decode('utf-8'))
    bytes = base64.b64decode(envelope['message']['data'])
    data = json.loads(bytes)
    subscription = envelope['subscription'].split('/')[-1]
    return data, subscription


def write_records_to_fs(records, collection_name, update_date_document_name=None, primary_key=None):
    logging.info(f"Writing {len(records)} to the firestore")
    batch = db.batch()

    for i, record in enumerate(records):
        batch.set(db.collection(collection_name).document(record[primary_key] if primary_key else None), record)
        if (i + 1) % config.BATCH_SIZE == 0:
            batch.commit()
            logging.info(f'Write {i} message(s) to the firestore')
    batch.commit()
    db.collection('Graphs').document(update_date_document_name). \
        set(dict(id=update_date_document_name, date=datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')))
    logging.info(f'Writing message to {collection_name} finished')


def write_logs_to_sql(logs):
    if logs:
        log_df = pd.DataFrame(logs)
        log_df['date'] = pd.to_datetime(log_df.date)
        log_df['from_value'] = log_df.from_value.astype(str)
        log_df['to_value'] = log_df.to_value.astype(str)
        log_df.to_sql('fc_transitie_log', con=sqlEngine, index=False, if_exists='append')


def write_records_to_sql(records, topic_config):
    name = topic_config.get('name')
    update_date_document_name = topic_config.get('update_date_document')
    table = topic_config.get('sql_table')
    logging.info(f"Processing {name}, writing {len(records)} to {table}")
    df = pd.DataFrame(records).replace({np.nan: None})
    datums = [col for col in df.columns if "datum" in col]
    df[datums] = df[datums].apply(
        lambda x: x.dt.strftime("%Y-%m-%d %H:%M-%S") if hasattr(x, 'dt') else pd.Series([None] * len(df)))
    df[datums] = df[datums].replace({'NaT': None})

    logging.info('made df')
    columns = ",".join(df.columns)
    values = [tuple(x for x in record) for record in df.values]
    duplicates = ",\n".join(f"{col}=values({col})" for col in df.columns)
    value_question_marks = ",".join(["%s"] * len(df.columns))
    update_query = f"""
INSERT INTO {table}
    ({columns})
values
   ({value_question_marks})
on duplicate key update
    {duplicates}
"""
    logging.info('created query')
    with sqlEngine.connect() as con:
        logging.info('created conn')
        result: ResultProxy = con.execute(update_query, *values)
    db.collection('Graphs').document(update_date_document_name). \
        set(dict(id=update_date_document_name, date=datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')))
    logging.info(f"{result.rowcount} where written to the database")


def get_secret(project_id, secret_id, version_id='latest'):
    client = secretmanager.SecretManagerServiceClient()
    name = client.secret_version_path(project_id, secret_id, version_id)
    response = client.access_secret_version(name)
    payload = response.payload.data.decode('UTF-8')
    return payload


@contextmanager
def sql_reference(record_ids=None):
    logging.info("creating sql reference")
    if record_ids is None:
        record_ids = []

    relevant_columns = list(config.HISTORY_COLUMNS.values()) + list(config.STATUS_CHANGE_COLUMNS.keys()) + list(
        set(itertools.chain(*[value.values() for value in config.STATUS_CHANGE_COLUMNS.values()])))

    if record_ids:
        sql = f"""
select {",".join(relevant_columns)}
from fc_aansluitingen fca
where fca.sleutel in ({",".join([f"'{record}'" for record in record_ids])})
"""  # nosec
        all_records = pd.read_sql(sql, sqlEngine)

        def get_aansluiting(sleutel):
            row = all_records[all_records.sleutel == sleutel]
            if row.empty:
                return None
            else:
                return row.to_dict(orient="records")[0]
    else:
        def get_aansluiting(sleutel):
            sql = f"""
select *
from fc_aansluitingen fca
where fca.sleutel = '{sleutel}'
"""  # nosec
            return pd.read_sql(sql, sqlEngine).to_dict(orient="records")[0]

    yield get_aansluiting


def prepare_records(records):  # noqa: C901
    logging.info("Preparing records")
    updated_records = []
    updated_log = []
    history_columns = config.HISTORY_COLUMNS
    status_change_columns = config.STATUS_CHANGE_COLUMNS
    primary_keys = config.PRIMARY_KEYS

    context = sql_reference([record[primary_keys] for record in records])
    datetime_format = '%Y-%m-%d %H:%M:%S'

    with context as get_aansluiting:
        df = pd.DataFrame(records)
        datums = [col for col in df.columns if "datum" in col]
        for datum in datums:
            df[datum] = df[datum].apply(pd.to_datetime,
                                        infer_datetime_format=True,
                                        errors="coerce",
                                        utc=True)
            df[datum] = df[datum].apply(lambda x: x.tz_convert(None) if x else x)
        for i, record in df.iterrows():
            record = dict(record)
            reference_record = get_aansluiting(record[primary_keys])
            if reference_record:

                # add date column to new record if already exists or if value = 1
                for column, date_column in history_columns.items():
                    if not isinstance(reference_record.get(date_column), NaTType):
                        record[date_column] = reference_record[date_column]
                    elif '1' in str(record[column]):
                        record[date_column] = datetime.now().strftime(datetime_format)

                # add transition log if the status changed
                for column, id_columns in status_change_columns.items():
                    if column in reference_record:
                        if (reference_record[column] != record[column]) and \
                                not (pd.isnull(reference_record[column]) and pd.isnull(record[column])):
                            updated_log.append(create_log(column, id_columns, record, reference_record))
                    else:  # if the column did not exists before, update log with 'primary entry'
                        updated_log.append(create_log(column, id_columns, record))

            else:  # if not exists update log with 'primary entry'
                for key, value in history_columns.items():
                    if '1' in str(record[key]):
                        record[value] = datetime.now().strftime(datetime_format)

            updated_records.append(record)

    logging.info(f"{len(updated_records)} records to be updated, {len(updated_log)} logs to be added")
    return updated_records, updated_log


def create_log(key, value, record, record_fs=None):
    return {
        'sleutel': record[value['sleutel']],
        'key': key,
        'from_value': record_fs[key] if record_fs else 'First entry',
        'to_value': record[key],
        'date': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'project': record[value['project']]
    }


if 'db_ip' in config.database:
    SACN = 'mysql+mysqlconnector://{}:{}@{}:3306/{}?charset=utf8&ssl_ca={}&ssl_cert={}&ssl_key={}'.format(
        config.database['db_user'],
        get_secret(config.database['project_id'], config.database['secret_name']),
        config.database['db_ip'],
        config.database['db_name'],
        config.database['server_ca'],
        config.database['client_ca'],
        config.database['client_key']
    )
else:
    SACN = 'mysql+pymysql://{}:{}@/{}?unix_socket=/cloudsql/{}:europe-west1:{}'.format(
        config.database['db_user'],
        get_secret(config.database['project_id'], config.database['secret_name']),
        config.database['db_name'],
        config.database['project_id'],
        config.database['instance_id']
    )

sqlEngine = create_engine(SACN, pool_recycle=3600)
