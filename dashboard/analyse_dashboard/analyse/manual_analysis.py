# before running this script make sure that:
# 1) you have an updated credential file for fttx production
# 2) checked out master
# Then press play :)

import os
import config
import time
import pandas as pd
from google.cloud import firestore_v1
from Analyse.KPNDFN import KPNETL
from Analyse.TMobile import TMobileETL
from Analyse.KPNDFN import DFNETL
import logging

logging.basicConfig(
    format=' %(asctime)s - %(name)s -%(levelname)s -%(filename)s:%(funcName)s:%(lineno)s - %(message)s',
    level=logging.INFO)

config.path_data_b = 'gs://vwt-p-gew1-fttx-dashboard-stg/'
config.client_config['kpn']['planning_location'] = \
    config.path_data_b + 'Forecast JUNI 2020_def.xlsx'
config.client_config['tmobile']['planning_location'] = \
    config.path_data_b + 'dfn_forecast_dummy.xlsx'
config.client_config['dfn']['planning_location'] = \
    config.path_data_b + 'Forecast JUNI 2020_def.xlsx'
config.client_config['kpn']['target_location'] = \
    config.path_data_b + '20200501_Overzicht bouwstromen KPN met indiendata offerte v12.xlsx'
config.client_config['tmobile']['target_location'] = \
    config.path_data_b + '20200501_Overzicht bouwstromen KPN met indiendata offerte v12.xlsx'
config.client_config['dfn']['target_location'] = \
    config.path_data_b + 'dfn_bouwstromen_dummy.xlsx'

keys = os.listdir(config.path_jsons)
for fn in keys:
    if 'p-gew1-fttx' in fn:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.path_jsons + fn

start = time.time()
client_name = "kpn"
kpn = KPNETL(client=client_name, config=config.client_config[client_name])
kpn.perform()
firestore_v1.Client().collection('Graphs').document('update_date_kpn').set({'id': 'update_date_kpn',
                                                                            'date': pd.Timestamp.now()})

client_name = "tmobile"
tmobile = TMobileETL(client=client_name, config=config.client_config[client_name])
tmobile.perform()
firestore_v1.Client().collection('Graphs').document('update_date_tmobile').set({'id': 'update_date_tmobile',
                                                                                'date': pd.Timestamp.now()})

client_name = "dfn"
dfn = DFNETL(client=client_name, config=config.client_config[client_name])
dfn.perform()
firestore_v1.Client().collection('Graphs').document('update_date_dfn').set({'id': 'update_date_dfn',
                                                                            'date': pd.Timestamp.now()})

print(str((time.time() - start) / 60) + ' min')
