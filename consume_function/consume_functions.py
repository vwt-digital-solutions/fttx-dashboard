import json
import base64
from datetime import datetime
import logging
from google.cloud import firestore_v1, secretmanager
from sqlalchemy.engine import ResultProxy

import config
from toggles import toggles
from sqlalchemy import create_engine
from contextlib import contextmanager
import pandas as pd

db = firestore_v1.Client()


def process_asbuilt(records, topic_config):
    logging.info("Processing asbuilt")
    collection_name = topic_config.get('firestore_collection')
    primary_key = topic_config.get('primary_key')
    update_date_document_name = topic_config.get('update_date_document')
    write_records_to_fs(records=records,
                        collection_name=collection_name,
                        update_date_document_name=update_date_document_name,
                        primary_key=primary_key)


def process_fiberconnect(records, topic_config):
    logging.info("Processing fiber connect")
    collection_name = topic_config.get('firestore_collection')
    primary_key = topic_config.get('primary_key')
    update_date_document_name = topic_config.get('update_date_document')
    records, logs = prepare_records(records)
    if toggles.fc_sql:
        write_records_to_sql(records)
    else:
        write_records_to_fs(records=records,
                            collection_name=collection_name,
                            update_date_document_name=update_date_document_name,
                            primary_key=primary_key)
    write_records_to_fs(records=logs,
                        collection_name='transitionlog',
                        update_date_document_name=update_date_document_name)


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
    if not toggles.consume_meters:
        db.collection('Graphs').document('update_date_consume').set(dict(
            id='update_date_consume', date=datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')))
    else:
        db.collection('Graphs').document(update_date_document_name).set(dict(
            id=update_date_document_name, date=datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')))
    logging.info(f'Writing message to {collection_name} finished')


def write_records_to_sql(records):
    logging.info(f"Writing {len(records)} to the database")

    columns = ",".join(list(records[0].keys()))
    values = ",\n".join(
        f"({x})" for x in
        [",".join(f"'{x}'" if x is not None else 'null' for x in record.values()) for record in records])
    duplicates = ",\n".join(f"{col}=values({col})" for col in records[0].keys())
    update_query = f"""
INSERT INTO fc_aansluitingen
    ({columns})
values
   {values}
on duplicate key update
    {duplicates}
"""

    with sqlEngine.connect() as con:
        result: ResultProxy = con.execute(update_query)
        logging.info(f"{result.rowcount} where written to the database")


def create_log(key, value, record, record_fs=None):
    return {
        'sleutel': record[value['sleutel']],
        'key': key,
        'from_value': record_fs[key] if record_fs else 'First entry',
        'to_value': record[key],
        'date': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'project': record[value['project']]
    }


def get_secret(project_id, secret_id, version_id='latest'):
    client = secretmanager.SecretManagerServiceClient()
    name = client.secret_version_path(project_id, secret_id, version_id)
    response = client.access_secret_version(name)
    payload = response.payload.data.decode('UTF-8')
    return payload


@contextmanager
def sql_reference(record_ids=None):
    if record_ids is None:
        record_ids = []

    if record_ids:
        sql = f"""
select *
from fc_aansluitingen fca
where fca.sleutel in ({",".join([f"'{record}'" for record in record_ids])})
"""  # nosec
        all_records = pd.read_sql(sql, sqlEngine)

        def get_aansluiting(sleutel):
            row = all_records[all_records.sleutel == sleutel]
            if row.empty:
                return None
            else:
                return row.to_dict(orient="records")
    else:
        def get_aansluiting(sleutel):
            sql = f"""
select *
from fc_aansluitingen fca
where fca.sleutel = '{sleutel}'
"""  # nosec
            return pd.read_sql(sql, sqlEngine).to_dict(orient="records")

    yield get_aansluiting


@contextmanager
def firestore_reference(firestore_collection):
    collection = db.collection(firestore_collection)

    def get_aansluiting(doc_id):
        return collection.document(doc_id).get().to_dict()

    yield get_aansluiting


def prepare_records(records):
    logging.info("Preparing records")
    updated_records = []
    updated_log = []
    history_columns = config.HISTORY_COLUMNS
    status_change_columns = config.STATUS_CHANGE_COLUMNS
    firestore_collection = config.FIRESTORE_COLLECTION
    primary_keys = config.PRIMARY_KEYS

    if toggles.fc_sql:
        context = sql_reference([record[primary_keys] for record in records])
        datetime_format = '%Y-%m-%d %H:%M:%S'
    else:
        context = firestore_reference(firestore_collection)
        datetime_format = '%Y-%m-%dT%H:%M:%SZ'

    with context as get_aansluiting:
        for record in records:
            print(record[primary_keys])
            record_fs = get_aansluiting(record[primary_keys])
            if record_fs:
                # add date column to new record if already exists or if value = 1
                for column, date_column in history_columns.items():
                    if date_column in record_fs:
                        record[date_column] = record_fs[date_column]
                    elif '1' in str(record[column]):
                        record[date_column] = datetime.now().strftime(datetime_format)

                # add transition log if the status changed
                for key, value in status_change_columns.items():
                    if key in record_fs:
                        if record_fs[key] != record[key]:
                            updated_log.append(create_log(key, value, record, record_fs))
                    else:  # if the column did not exists before, update log with 'primary entry'
                        updated_log.append(create_log(key, value, record))

            else:  # if not exists update log with 'primary entry'
                for key, value in status_change_columns.items():
                    updated_log.append(create_log(key, value, record))
                for key, value in history_columns.items():
                    if '1' in str(record[key]):
                        record[value] = datetime.now().strftime(datetime_format)

            updated_records.append(record)

    logging.info(f"{len(updated_records)} records to be updated, {len(updated_log)} logs to be added")
    return updated_records, updated_log


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
