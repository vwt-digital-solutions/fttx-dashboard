import json
import base64
from datetime import datetime
import logging
from google.cloud import firestore, secretmanager
from sqlalchemy.engine import ResultProxy
from FiberconnectAansluitingen import ConsumeAansluitingenHistory, ConsumeAansluitingen

import config
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

db = firestore.Client()


def process_fiberconnect(records, topic_config):
    logging.info("Processing fiber connect")
    df = pd.DataFrame(records).replace({np.nan: None})
    consume_aansluitingen = ConsumeAansluitingen(records=df, sql_engine=sqlEngine)
    consume_aansluitingen.consume_records()
    consume_aansluitingen_history = ConsumeAansluitingenHistory(records=df, sql_engine=sqlEngine)
    consume_aansluitingen_history.consume_records()

    update_date_document_name = topic_config.get('update_date_document')
    db.collection('Graphs').document(update_date_document_name). \
        set(dict(id=update_date_document_name, date=datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')))


def process_default(records, topic_config):
    logging.info('Processing default')
    df = pd.DataFrame(records).replace({np.nan: None})
    datums = [col for col in df.columns if "datum" in col]
    for datum in datums:
        df[datum] = df[datum].apply(pd.to_datetime,
                                    infer_datetime_format=True,
                                    errors="coerce",
                                    utc=True)
        df[datum] = df[datum].apply(lambda x: x.tz_convert(None) if x else x)

    records = df.to_dict(orient='records')
    write_records_to_sql(records=records,
                         topic_config=topic_config)


def parse_request(request):
    envelope = json.loads(request.data.decode('utf-8'))
    bytes = base64.b64decode(envelope['message']['data'])
    data = json.loads(bytes)
    subscription = envelope['subscription'].split('/')[-1]
    return data, subscription


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
