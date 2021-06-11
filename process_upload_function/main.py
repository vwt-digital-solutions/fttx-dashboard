import json
from datetime import datetime
import pandas as pd
import numpy as np
import logging
from google.cloud import firestore, secretmanager, storage
from sqlalchemy import create_engine
from sqlalchemy.engine import ResultProxy
import config
import traceback

db = firestore.Client()


def handler(data, context):
    logging.info('Run started')
    bucket = data['bucket']
    filename = data['name']
    try:
        if 'uploads/bouwportaal_orders/' in filename:
            df = data_from_store(bucket, filename)
            process_bouwportaal_orders(df)
            logging.info('Run finished')
            return 'OK', 200
        else:
            logging.info(f'Skipping {filename} because it does not need processing')
            logging.info('Run finished')
            return 'OK', 200
    except Exception as e:
        logging.error(f'Processing {filename} stopped')
        logging.error(f'Processing failure: {e}')
        traceback.print_exc()
        return 'Error', 500


def process_bouwportaal_orders(df):
    datums = [col for col in df.columns if "datum" in col]
    for datum in datums:
        df[datum] = df[datum].apply(pd.to_datetime,
                                    infer_datetime_format=True,
                                    errors="coerce",
                                    utc=True)
        df[datum] = df[datum].apply(lambda x: x.tz_convert(None) if x else x)
    df.rename(columns=config.bouwportaal_orders_column_mapping, inplace=True)
    df = df.astype(str).replace({np.nan: None, 'NaT': None, 'nan': None})
    table = config.upload_config['bouwportaal_orders']['database_table']
    write_to_sql(df, table)


def data_from_store(bucket_name, blob_name):
    path = 'gs://{}/{}'.format(bucket_name, blob_name)
    if blob_name.endswith('.xlsx'):
        df = pd.ExcelFile(path, dtype=str)
    elif blob_name.endswith('.json'):
        bucket = storage.Client().get_bucket(bucket_name)
        blob = storage.Blob(blob_name, bucket)
        content = blob.download_as_string()
        data = json.loads(content.decode('utf-8'))
        df = pd.DataFrame.from_records(data['data'])
    else:
        raise ValueError('File is not json or xlsx: {}'.format(blob_name))
    logging.info('Read file {} from {}'.format(blob_name, bucket_name))
    return df


def write_to_sql(df, table):
    logging.info(f"Processing 'Bouwportaal orders', writing {len(df)} to {table}")
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

    db.collection('Graphs').document("update_date_bouwportaal_orders"). \
        set(dict(id="update_date_bouwportaal_orders", date=datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')))
    logging.info(f"{result.rowcount} where written to the database")


def get_secret(project_id, secret_id, version_id='latest'):
    client = secretmanager.SecretManagerServiceClient()
    name = client.secret_version_path(project_id, secret_id, version_id)
    response = client.access_secret_version(name)
    payload = response.payload.data.decode('UTF-8')
    return payload


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
