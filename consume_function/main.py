import json
import base64
import config
import logging
from datetime import datetime

from google.cloud import firestore_v1

from toggles import ReleaseToggles

db = firestore_v1.Client()

toggles = ReleaseToggles('toggles.yaml')


def handler(request):

    try:
        envelope = json.loads(request.data.decode('utf-8'))
        bytes = base64.b64decode(envelope['message']['data'])
        data = json.loads(bytes)
        subscription = envelope['subscription'].split('/')[-1]
        topic_config = config.topic_config.get(subscription)
        if not toggles.consume_meters:
            records = data['it-fiber-connect-new-construction']
            collection_name = config.FIRESTORE_COLLECTION
            primary_key = config.PRIMARY_KEYS if hasattr(config, 'PRIMARY_KEYS') else None
            logging.info(f'Read message from subscription {subscription}')
            records, logs = prepare_records(records)
            write_records_to_fs(records=records, collection_name=collection_name, primary_key=primary_key)
            write_records_to_fs(records=logs, collection_name='transitionlog')
        else:
            records = data[topic_config.get('subject')]
            collection_name = topic_config.get('firestore_collection')
            primary_key = topic_config.get('primary_key') if 'primary_key' in topic_config else None
            update_date_document_name = topic_config.get('update_date_document')
            logging.info(f'Read message from subscription {subscription}')

            if topic_config.get('name') == 'fiberconnect':
                records, logs = prepare_records(records)
                write_records_to_fs(records=records,
                                    collection_name=collection_name,
                                    update_date_document_name=update_date_document_name,
                                    primary_key=primary_key)
                write_records_to_fs(records=logs,
                                    collection_name='transitionlog',
                                    update_date_document_name=update_date_document_name)
            elif topic_config.get('name') == 'asbuilt-meters':
                write_records_to_fs(records=records,
                                    collection_name=collection_name,
                                    update_date_document_name=update_date_document_name,
                                    primary_key=primary_key)

    except Exception as e:
        logging.error(f'Extracting of data failed: {e}')
        return 'Error', 500

    return 'OK', 200


def write_records_to_fs(records, collection_name, update_date_document_name=None, primary_key=None):

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


def create_log(key, value, record, record_fs=None):
    return {
        'sleutel': record[value['sleutel']],
        'key': key,
        'from_value': record_fs[key] if record_fs else 'First entry',
        'to_value': record[key],
        'date': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'project': record[value['project']]
    }


def prepare_records(records):

    updated_records = []
    updated_log = []
    history_columns = config.HISTORY_COLUMNS
    status_change_columns = config.STATUS_CHANGE_COLUMNS
    firestore_collection = config.FIRESTORE_COLLECTION
    primary_keys = config.PRIMARY_KEYS

    for record in records:
        record_fs = db.collection(firestore_collection).document(record[primary_keys]).get()
        if record_fs.exists:
            record_fs = record_fs.to_dict()

            # add date column to new record if already exists or if value = 1
            for key, value in history_columns.items():
                if value in record_fs:
                    record[value] = record_fs[value]
                elif '1' in str(record[key]):
                    record[value] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

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
                    record[value] = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')

        updated_records.append(record)

    return updated_records, updated_log
