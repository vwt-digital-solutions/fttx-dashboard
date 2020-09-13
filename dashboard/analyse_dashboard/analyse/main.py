# %% Set data path
import json
import base64
from gobits import Gobits
import config
from google.cloud import pubsub, firestore
from Record import DocumentListRecord, ListRecord
from Analyse.KPN import KPNETL
from Analyse.TMobile import TMobileETL
from functions import set_date_update, get_data, masks_phases

import logging

logging.basicConfig(level=logging.INFO)

publisher = pubsub.PublisherClient()


def analyse(request):
    try:
        analyseKPN('kpn')
        publish_project_data(request)
        analyseTmobile('t-mobile')
        set_date_update()
        return 'OK', 200

    except Exception as e:
        logging.exception(f'Analyse failed {e}')
        return 'Error', 500

    finally:
        logging.info('run done')


def analyseKPN(client_name):
    kpn = KPNETL(client=client_name, config=config.client_config[client_name])
    kpn.perform()


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
        dlr.to_firestore(client="KPN")

        lr = ListRecord(dict(bar_names=bar_names), collection="Data")
        lr.to_firestore(graph_name="bar_names", client="KPN")

        logging.info(f'masks bar uploaded for {project}')
    except Exception:
        logging.exception('Graph calculation failed')


def get_project_list():
    # We could get this list from the config file
    data = [
        el['label'] for el in
        firestore.Client().collection('Data').document('kpn_project_names').get().to_dict()['record']['filters']
    ]
    return data


def publish_project_data(request):
    data = get_project_list()
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
