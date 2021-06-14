import logging
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import firestore_v1

import config
from Analyse.Capacity_analysis.Analysis_capacity import CapacityETL
from Analyse.Finance_ETL import FinanceETL
from Analyse.IndicatorAnalysis import (DFNIndicatorETL, KPNIndicatorETL,
                                       TmobileIndicatorETL, KPNActivatieIndicatorETL)
from Analyse.ProjectInfoETL import ProjectInfoETL
from functions import set_date_update
from toggles import ReleaseToggles

logging.basicConfig(level=logging.INFO)

db = firestore_v1.Client()

toggles = ReleaseToggles("toggles.yaml")


def analyse_kpn_1(request):
    try:
        if get_update_dates("kpn_1"):
            analyseKPN_1()
            set_date_update("kpn_1")
            return "OK", 200
        else:
            logging.info("Analyse KPN skipped, already up to date")
            return "OK", 200
    except Exception as e:
        logging.exception(f"Analyse KPN failed {e}")
        return "Error", 500
    finally:
        logging.info("run done")


def analyse_kpn_2(request):
    try:
        if get_update_dates("kpn_2"):
            analyseKPN_2()
            set_date_update("kpn_2")
            return "OK", 200
        else:
            logging.info("Analyse KPN skipped, already up to date")
            return "OK", 200
    except Exception as e:
        logging.exception(f"Analyse KPN failed {e}")
        return "Error", 500
    finally:
        logging.info("run done")


def analyse_tmobile(request):
    try:
        if get_update_dates("tmobile"):
            analyseTmobile()
            set_date_update("tmobile")
            return "OK", 200
        else:
            logging.info("Analyse T-Mobile skipped, already up to date")
            return "OK", 200
    except Exception as e:
        logging.exception(f"Analyse T-Mobile failed {e}")
        return "Error", 500
    finally:
        logging.info("run done")


def analyse_dfn(request):
    try:
        if get_update_dates("dfn"):
            analyseDFN()
            set_date_update("dfn")
            return "OK", 200
        else:
            logging.info("Analyse DFN skipped, already up to date")
            return "OK", 200
    except Exception as e:
        logging.exception(f"Analyse DFN failed {e}")
        return "Error", 500
    finally:
        logging.info("run done")


def analyse_capacity_kpn(request):
    try:
        if get_update_dates("capacity_kpn"):
            analyseCapacity("kpn")
            set_date_update("capacity_kpn")
            return "OK", 200
        else:
            logging.info("Capacity analysis KPN skipped, already up to date")
            return "OK", 200
    except Exception as e:
        logging.exception(f"Capacity analysis KPN failed {e}")
        return "Error", 500
    finally:
        logging.info("run done")


def finance_analyse_kpn(request):
    try:
        analyseFinance("kpn")
        set_date_update("kpn_finance")
        return "OK", 200
    except Exception as e:
        logging.exception(f"Finance analyse KPN failed {e}")
        return "Error", 500
    finally:
        logging.info("run done")


def project_info_update_kpn(request):
    try:
        analyseProjectInfo("kpn")
        set_date_update("kpn_projectinfo")
        return "OK", 200
    except Exception as e:
        logging.exception(f"Projectinfo analyse KPN failed {e}")
        return "Error", 500
    finally:
        logging.info("run done")


def bouwportaal_analyse_kpn(request):
    try:
        analyseBouwportaalKPN()
        set_date_update("kpn_bouwportaal")
        return "OK", 200
    except Exception as e:
        logging.exception((f"KPN Bouwportaal analyse failed {e}"))
        return "Error", 500
    finally:
        logging.info('Run done')


def analyseKPN_1():
    kpn = KPNIndicatorETL(client="kpn", config=config.client_config["kpn"])
    kpn.perform_1()


def analyseKPN_2():
    kpn = KPNIndicatorETL(client="kpn", config=config.client_config["kpn"])
    kpn.perform_2()


def analyseProjectInfo(client_name):
    projectinfo_kpn = ProjectInfoETL(
        client=client_name, config=config.client_config[client_name]
    )
    projectinfo_kpn.perform()


def analyseBouwportaalKPN():
    kpn = KPNActivatieIndicatorETL(client='kpn', config=config.client_config['kpn'])
    kpn.perform()


def analyseCapacity(client_name):
    cpc = CapacityETL(client=client_name, config=config.client_config[client_name])
    cpc.perform()


def analyseFinance(client_name):
    finance = FinanceETL(
        client_name=client_name, config=config.client_config[client_name]
    )
    finance.perform()


def analyseDFN():
    dfn = DFNIndicatorETL(client="dfn", config=config.client_config["dfn"])
    dfn.perform()


def analyseTmobile():
    tmobile = TmobileIndicatorETL(
        client="tmobile", config=config.client_config["tmobile"]
    )
    tmobile.perform()


def str_to_datetime(str_to_parse):
    return pd.to_datetime(str_to_parse, errors="coerce", infer_datetime_format=True)


def get_update_dates(client):
    check = (
        db.collection("Graphs").document("update_date_fiberconnect").get().exists
        & db.collection("Graphs").document(f"update_date_{client}").get().exists
    )
    if not check:
        return True
    latest_consume = str_to_datetime(
        db.collection("Graphs")
        .document("update_date_fiberconnect")
        .get()
        .to_dict()
        .get("date")
    )
    latest_analysis = str_to_datetime(
        db.collection("Graphs")
        .document(f"update_date_{client}")
        .get()
        .to_dict()
        .get("date")
    )
    if ((datetime.now() - latest_consume) > timedelta(minutes=5)) and (
        latest_analysis < latest_consume
    ):
        return True
    else:
        return False
