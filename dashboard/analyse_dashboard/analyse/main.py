import config
from Analyse.Capacity_analysis.Analysis_capacity import CapacityETL
from Analyse.Finance_ETL import FinanceETL
from Analyse.KPNIndicatorAnalysis import TmobileIndicatorETL, DFNIndicatorETL, KPNIndicatorETL
from Analyse.ProjectInfoETL import ProjectInfoETL
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


def analyse_capacity_kpn(request):
    try:
        if get_update_dates('capacity_kpn'):
            analyseCapacity('kpn')
            set_date_update('capacity_kpn')
            return 'OK', 200
        else:
            logging.info('Capacity analysis KPN skipped, already up to date')
            return 'OK', 200
    except Exception as e:
        logging.exception(f'Capacity analysis KPN failed {e}')
        return 'Error', 500
    finally:
        logging.info('run done')


def finance_analyse_kpn(request):
    try:
        analyseFinance('kpn')
        set_date_update('kpn_finance')
        return 'OK', 200
    except Exception as e:
        logging.exception(f'Finance analyse KPN failed {e}')
        return 'Error', 500
    finally:
        logging.info('run done')


def analyseKPN(client_name):
    kpn = KPNIndicatorETL(client=client_name, config=config.client_config[client_name])
    kpn.perform()
    projectinfo_kpn = ProjectInfoETL(client=client_name, config=config.client_config[client_name])
    projectinfo_kpn.perform()


def analyseCapacity(client_name):
    cpc = CapacityETL(client=client_name, config=config.client_config[client_name])
    cpc.perform()


def analyseFinance(client_name):
    finance = FinanceETL(client_name=client_name, config=config.client_config[client_name])
    finance.perform()


def analyseDFN(client_name):
    dfn = DFNIndicatorETL(client=client_name, config=config.client_config[client_name])
    dfn.perform()
    # projectinfo_dfn = ProjectInfoETL(client=client_name, config=config.client_config[client_name])
    # projectinfo_dfn.perform()


def analyseTmobile(client_name):
    tmobile = TmobileIndicatorETL(client=client_name, config=config.client_config[client_name])
    tmobile.perform()
    # projectinfo_tmobile = ProjectInfoETL(client=client_name, config=config.client_config[client_name])
    # projectinfo_tmobile.perform()


def str_to_datetime(str_to_parse):
    return pd.to_datetime(str_to_parse, errors='coerce', infer_datetime_format=True)


def get_update_dates(client):
    check = (db.collection('Graphs').document('update_date_fiberconnect').get().exists
             & db.collection('Graphs').document(f'update_date_{client}').get().exists)
    if not check:
        return True
    latest_consume = str_to_datetime(
        db.collection('Graphs').
        document('update_date_fiberconnect').
        get().to_dict().get('date'))
    latest_analysis = str_to_datetime(
        db.collection('Graphs').
        document(f'update_date_{client}').
        get().to_dict().get('date'))
    if ((datetime.now() - latest_consume) > timedelta(minutes=5)) and (latest_analysis < latest_consume):
        return True
    else:
        return False
