from Analyse.FttX import PickleExtract
from Analyse.TMobile import TMobileETL, TMobileTestETL
from Analyse.KPN import KPNTestETL, KPNETL
from Analyse.DFN import DFNTestETL, DFNETL
import os
import config
import logging

logging.basicConfig(
    format=' %(asctime)s - %(name)s -%(levelname)s - %(filename)s:%(funcName)s:%(lineno)s - %(message)s',
    level=logging.INFO
)


class KPNPickleETL(PickleExtract, KPNETL):
    pass


class TMobilePickleETL(PickleExtract, TMobileETL):
    pass


class DFNPickleETL(PickleExtract, DFNETL):
    pass


if 'FIRESTORE_EMULATOR_HOST' in os.environ:
    client_name = "kpn"
    kpn = KPNPickleETL(client=client_name, config=config.client_config[client_name])
    kpn.perform()
    client_name = "tmobile"
    tmobile = TMobilePickleETL(client=client_name, config=config.client_config[client_name])
    tmobile.perform()
    client_name = "dfn"
    dfn = DFNPickleETL(client=client_name, config=config.client_config[client_name])
    dfn.perform()
else:
    client_name = "kpn"
    kpn = KPNTestETL(client=client_name, config=config.client_config[client_name])
    kpn.perform()
    client_name = "tmobile"
    tmobile = TMobileTestETL(client=client_name, config=config.client_config[client_name])
    tmobile.perform()
    client_name = "dfn"
    dfn = DFNTestETL(client=client_name, config=config.client_config[client_name])
    dfn.perform()
