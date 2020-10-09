# %% Set data path
import json
import base64
from gobits import Gobits
import config
from google.cloud import pubsub
from Analyse.Record import DocumentListRecord, ListRecord
from Analyse.KPN import KPNETL
from Analyse.TMobile import TMobileETL
from Analyse.DFN import DFNETL
from functions import set_date_update, get_data, masks_phases
from datetime import datetime, timedelta
from google.cloud import firestore_v1
import pandas as pd

import logging

logging.basicConfig(level=logging.INFO)

publisher = pubsub.PublisherClient()

db = firestore_v1.Client()


def analyse(request):
    try:
        latest_consume = str_to_datetime(
            db.collection('Graphs').
            document('update_date_consume').
            get().to_dict().get('date'))
        latest_analysis = str_to_datetime(
            db.collection('Graphs').
            document('update_date').
            get().to_dict().get('date'))

        if ((datetime.now() - latest_consume) > timedelta(minutes=5)) and (latest_analysis < latest_consume):
            publish_project_data(request, 'kpn')
            analyseKPN('kpn')
            analyseTmobile('t-mobile')
            set_date_update()
            analyseDFN('dfn')
            publish_project_data(request, 'dfn')
            return 'OK', 200
        else:
            logging.info('Analyse skipped, already up to date')
            return 'OK', 200

    except Exception as e:
        logging.exception(f'Analyse failed {e}')
        return 'Error', 500

    finally:
        logging.info('run done')


def analyseKPN(client_name):
    kpn = KPNETL(client=client_name, config=config.client_config[client_name])
    kpn.perform()


def analyseDFN(client_name):
    dfn = DFNETL(client=client_name, config=config.client_config[client_name])
    dfn.perform()


def analyseTmobile(client_name):
    tmobile = TMobileETL(client=client_name, config=config.client_config[client_name])
    tmobile.perform()


def graph(request):
    try:
        envelope = json.loads(request.data.decode('utf-8'))
        bytes = base64.b64decode(envelope['message']['data'])
        project = json.loads(bytes)
        df_l = get_data([project], config.col, None, None, 0)
        bar_names, document_list = masks_phases(project, df_l)
        dlr = DocumentListRecord(document_list, collection="Data", document_key=['filter', 'project'])
        dlr.to_firestore(client="kpn")

        lr = ListRecord(dict(bar_names=bar_names), collection="Data")
        lr.to_firestore(graph_name="bar_names", client="kpn")

        logging.info(f'masks bar uploaded for {project}')
    except Exception:
        logging.exception('Graph calculation failed')


def get_project_list(client):
    data = config.client_config[client].get('projects')
    return data


def publish_project_data(request, client):
    data = get_project_list(client)
    gobits = Gobits.from_request(request=request)
    i = 1
    for msg in data:
        publish_json(gobits, msg_data=msg, rowcount=i, rowmax=len(data), **config.TOPIC_SETTINGS)
        i += 1


def publish_json(gobits, msg_data, rowcount, rowmax, topic_project_id, topic_name, subject=None):
    topic_path = publisher.topic_path(topic_project_id, topic_name)
    if subject:
        msg = {
            "gobits": [gobits.to_json()],
            subject: msg_data
        }
    else:
        msg = msg_data
        logging.info(f'Publish to {topic_path}: {msg}')
    future = publisher.publish(
        topic_path, bytes(json.dumps(msg).encode('utf-8')))
    future.add_done_callback(
        lambda x: logging.debug(
            'Published msg with ID {} ({}/{} rows).'.format(
                future.result(), rowcount, rowmax))
    )


def str_to_datetime(str):
    return pd.to_datetime(str, errors='coerce', infer_datetime_format=True)
