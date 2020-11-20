import os
import config
from Analyse.KPNDFN import KPNETL
from Analyse.TMobile import TMobileETL
from Analyse.KPNDFN import DFNETL


# make sure you have an updated credential file for fttx production, checked out master and press play
keys = os.listdir(config.path_jsons)
for fn in keys:
    if 'p-gew1-fttx' in fn:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.path_jsons + fn

client_name = "kpn"
kpn = KPNETL(client=client_name, config=config.client_config[client_name])
kpn.perform()

client_name = "tmobile"
tmobile = TMobileETL(client=client_name, config=config.client_config[client_name])
tmobile.perform()

client_name = "dfn"
dfn = DFNETL(client=client_name, config=config.client_config[client_name])
dfn.perform()
