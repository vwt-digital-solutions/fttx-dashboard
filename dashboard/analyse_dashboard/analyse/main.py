import config
from google.cloud import pubsub
from Analyse.KPN import KPNETL
from Analyse.TMobile import TMobileETL
from Analyse.DFN import DFNETL
from functions import set_date_update
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
            analyseKPN('kpn')
            analyseTmobile('tmobile')
            analyseDFN('dfn')
            set_date_update()
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


def str_to_datetime(str):
    return pd.to_datetime(str, errors='coerce', infer_datetime_format=True)
