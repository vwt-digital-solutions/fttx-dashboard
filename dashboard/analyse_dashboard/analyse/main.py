import config
from Analyse.KPNDFN import KPNETL, DFNETL
from Analyse.TMobile import TMobileETL
from functions import set_date_update
from datetime import datetime, timedelta
from google.cloud import firestore_v1
import pandas as pd

import logging

from toggles import ReleaseToggles

logging.basicConfig(level=logging.INFO)

db = firestore_v1.Client()

toggles = ReleaseToggles('toggles.yaml')


def analyse_kpn(request):
    try:
        if get_update_dates('kpn'):
            analyseKPN('kpn')
            set_date_update('kpn')
            return 'OK', 200
        else:
            logging.info('Analyse KPN skipped, already up to date')
            return 'OK', 200
    except Exception as e:
        logging.exception(f'Analyse KPN failed {e}')
        return 'Error', 500
    finally:
        logging.info('run done')


def analyse_tmobile(request):
    try:
        if get_update_dates('tmobile'):
            analyseTmobile('tmobile')
            set_date_update('tmobile')
            return 'OK', 200
        else:
            logging.info('Analyse T-Mobile skipped, already up to date')
            return 'OK', 200
    except Exception as e:
        logging.exception(f'Analyse T-Mobile failed {e}')
        return 'Error', 500
    finally:
        logging.info('run done')


def analyse_dfn(request):
    try:
        if get_update_dates('dfn'):
            analyseDFN('dfn')
            set_date_update('dfn')
            return 'OK', 200
        else:
            logging.info('Analyse DFN skipped, already up to date')
            return 'OK', 200
    except Exception as e:
        logging.exception(f'Analyse DFN failed {e}')
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


def get_update_dates(client):
    check = ((db.collection('Graphs').document('update_date_consume').get().exists) and
             (db.collection('Graphs').document(f'update_date_{client}').get().exists))
    if not check:
        return True
    latest_consume = str_to_datetime(
        db.collection('Graphs').
        document('update_date_consume').
        get().to_dict().get('date'))
    latest_analysis = str_to_datetime(
        db.collection('Graphs').
        document(f'update_date_{client}').
        get().to_dict().get('date'))
    if ((datetime.now() - latest_consume) > timedelta(minutes=5)) and (latest_analysis < latest_consume):
        return True
    else:
        return False
