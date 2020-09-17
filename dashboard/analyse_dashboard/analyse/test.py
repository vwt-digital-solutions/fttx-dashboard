# %% Initialize
import os
import time
import config
from Analyse.KPN import KPNETL
from functions import graph_overview
import logging

# from Analyse.TMobile import TMobileETL

logging.basicConfig(format=' %(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

# %% Set environment variables and permissions and data path
keys = os.listdir(config.path_jsons)
for fn in keys:
    if ('-d-' in fn) & ('-fttx-' in fn):
        gpath_d = config.path_jsons + fn
    if ('-p-' in fn) & ('-fttx-' in fn):
        gpath_p = config.path_jsons + fn
    if ('-d-' in fn) & ('-it-fiber' in fn):
        gpath_i = config.path_jsons + fn
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gpath_d

# %% Get data from state collection Projects
t_start = time.time()

client_name = "kpn"
kpn = KPNETL(client=client_name, config=config.client_config[client_name])
kpn.extract()
kpn.transform()
kpn.analyse()
kpn.load()

kpn.perform()

kpn._calculate_projectspecs()
kpn._prognose()
kpn._targets()
kpn._overview()
kpn._calculate_graph_overview()
kpn.intermediate_results.keys()
kpn.intermediate_results.y_target_l
kpn.record_dict['graph_targets_W']

df_prog = kpn.intermediate_results.df_prog
df_target = kpn.intermediate_results.df_target
df_real = kpn.intermediate_results.df_real
df_plan = kpn.intermediate_results.df_plan
HC_HPend = kpn.intermediate_results.HC_HPend
HAS_werkvoorraad = kpn.intermediate_results.HAS_werkvoorraad
res = 'W-MON'

graph_targets_W = graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad, res)

# kpn.perform()
# logging.info("KPN Done")

# client_name = "t-mobile"
# tmobile = TMobileETL(client=client_name, config=config.client_config[client_name])
# tmobile.perform()
# logging.info("T-mobile Done")
# logging.info(f"Analysis done. Took {time.time() - t_start} seconds")
