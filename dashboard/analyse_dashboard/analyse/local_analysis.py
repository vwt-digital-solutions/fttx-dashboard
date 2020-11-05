# %%
import logging
import os

import config
from Analyse.KPNDFN import DFNTestETL, DFNLocalETL
from Analyse.KPNDFN import KPNTestETL, KPNLocalETL
from Analyse.TMobile import TMobileTestETL, TMobileLocalETL

logging.basicConfig(
    format=' %(asctime)s - %(name)s -%(levelname)s - %(filename)s:%(funcName)s:%(lineno)s - %(message)s',
    level=logging.INFO
)


if 'FIRESTORE_EMULATOR_HOST' in os.environ:
    logging.info('writing to local firestore')
    client_name = "kpn"
    kpn = KPNLocalETL(client=client_name, config=config.client_config[client_name])
    kpn.perform()
    client_name = "tmobile"
    tmobile = TMobileLocalETL(client=client_name, config=config.client_config[client_name])
    tmobile.perform()
    client_name = "dfn"
    dfn = DFNLocalETL(client=client_name, config=config.client_config[client_name])
    dfn.perform()
else:
    logging.info('testing ETL, not writing to firestore')
    client_name = "kpn"
    kpn = KPNTestETL(client=client_name, config=config.client_config[client_name])
    kpn.perform()
    client_name = "tmobile"
    tmobile = TMobileTestETL(client=client_name, config=config.client_config[client_name])
    tmobile.perform()
    client_name = "dfn"
    dfn = DFNTestETL(client=client_name, config=config.client_config[client_name])
    dfn.perform()

# %%
