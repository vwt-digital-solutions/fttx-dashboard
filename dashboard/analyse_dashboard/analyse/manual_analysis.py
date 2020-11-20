# make sure you have an updated credential file for fttx production, checked out master and press play
import os
import config
from Analyse.KPNDFN import KPNETL
from Analyse.TMobile import TMobileETL
from Analyse.KPNDFN import DFNETL

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
    if 'd-gew1-fttx' in fn:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.path_jsons + fn

client_name = "kpn"
kpn = KPNETL(client=client_name, config=config.client_config[client_name])
kpn.extract()
kpn.perform()

client_name = "tmobile"
tmobile = TMobileETL(client=client_name, config=config.client_config[client_name])
tmobile.perform()

client_name = "dfn"
dfn = DFNETL(client=client_name, config=config.client_config[client_name])
dfn.perform()
