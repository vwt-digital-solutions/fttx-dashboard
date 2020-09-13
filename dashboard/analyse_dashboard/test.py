# %% Initialize
# import os
import time
import analyse.config as config
from Analyse.KPN import KPNTestETL
import logging

from Analyse.TMobile import TMobileTestETL

logging.basicConfig(format=' %(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

# %% Set environment variables and permissions and data path
# keys = os.listdir(config.path_jsons)
# for fn in keys:
#     if ('-d-' in fn) & ('-fttx-' in fn):
#         gpath_d = config.path_jsons + fn
#     if ('-p-' in fn) & ('-fttx-' in fn):
#         gpath_p = config.path_jsons + fn
#     if ('-d-' in fn) & ('-it-fiber' in fn):
#         gpath_i = config.path_jsons + fn

# %% Get data from state collection Projects
t_start = time.time()
# df_l, t_s, x_d, tot_l = get_data_FC(config.subset_KPN_2020, config.col, gpath_i, config.path_data, 0)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gpath_d

client_name = "kpn"
kpn = KPNTestETL(client=client_name, config=config.client_config[client_name])
kpn.perform()
logging.info("KPN Done")

client_name = "t-mobile"
tmobile = TMobileTestETL(client=client_name, config=config.client_config[client_name])
tmobile.perform()
logging.info("T-mobile Done")
logging.info(f"Analysis done. Took {time.time() - t_start} seconds")
