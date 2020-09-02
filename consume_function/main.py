import json
import base64
import config
import logging
import datetime

from google.cloud import firestore_v1

db = firestore_v1.Client()


def handler(request):

    try:
        envelope = json.loads(request.data.decode('utf-8'))
        bytes = base64.b64decode(envelope['message']['data'])
        data = json.loads(bytes)
        subscription = envelope['subscription'].split('/')[-1]
        records = data['it-fiber-connect-new-construction']
        collection_name = config.FIRESTORE_COLLECTION
        document_name_column = config.PRIMARY_KEYS if hasattr(config, 'PRIMARY_KEYS') else None
        logging.info(f'Read message from subscription {subscription}')

        write_records_to_fs(records, collection_name, document_name_column)

    except Exception as e:
        logging.error(f'Extracting of data failed: {e}')
        return 'Error', 500

    return 'OK', 200


def write_records_to_fs(records, collection_name, document_name_column=None):

    batch = db.batch()

    for i, record in enumerate(records):
        batch.set(db.collection(collection_name).document(record[document_name_column] if document_name_column else None), record)
        if (i + 1) % config.BATCH_SIZE == 0:
            batch.commit()
            logging.info(f'Write {i} message(s) to the firestore')
    batch.commit()
    db.collection('Graphs').document('update_date_consume').set(dict(
        id='update_date_consume', date=datetime.datetime.now().strftime('%Y-%m-%d')))
    logging.info('Writing message to firestore finished')
