# %% Initialize
from Analyse.TMobile import TMobileETL
from Analyse.KPN import KPNTestETL
from Analyse.DFN import DFNTestETL, DFNETL
import os
import time
import config
from Analyse.KPN import KPNETL, PickleExtract
from functions import graph_overview
import logging

logging.basicConfig(format=' %(asctime)s - %(name)s -%(levelname)s - %(filename)s:%(funcName)s:%(lineno)s - %(message)s',
                    level=logging.INFO)

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


class KPNPickleETL(PickleExtract, KPNETL):
    pass


client_name = "kpn"
kpn = KPNPickleETL(client=client_name, config=config.client_config[client_name])
kpn.extract()
kpn.transform()
kpn.analyse()
kpn.load()

# %%
kpn.perform()

kpn._calculate_projectspecs()
kpn._prognose()
kpn._targets()
kpn._overview()
kpn._calculate_graph_overview()
kpn.intermediate_results.keys()
kpn.intermediate_results.y_target_l
kpn.extracted_data.ftu['date_FTU0']
kpn.extracted_data.ftu['date_FTU1']
kpn.record_dict['graph_targets_W']
kpn.record_dict['analysis'].record

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
# firestore.Client().collection('Data').document('xxx').delete()
# firestore.Client().collection('Data').document('analysis').set(rec)
# docs = firestore.Client().collection('Data').where('id', '==', 'analysis').get()
# for doc in docs:
#     rec = doc.to_dict()


class TMobilePickleETL(PickleExtract, TMobileETL):
    pass


client_name = "tmobile"
tmobile = TMobileETL(client=client_name, config=config.client_config[client_name])
tmobile.perform()
logging.info("T-mobile Done")
logging.info(f"Analysis done. Took {time.time() - t_start} seconds")


# to test dfn
class DFNPickleETL(PickleExtract, DFNETL):
    pass


client_name = "dfn"
dfn = DFNPickleETL(client=client_name, config=config.client_config[client_name])
dfn.extract()
dfn.transform()
dfn.analyse()
dfn.load()

# %% test jaaroverzicht

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ''
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
kpn = KPNTestETL(client='kpn', config=config.client_config['kpn'])
kpn.extract()
kpn.transform()
kpn._calculate_projectspecs()
kpn._calculate_y_voorraad_act()
kpn._prognose()
# kpn._set_input_fields()
kpn._targets()
kpn._performance_matrix()
kpn._prognose_graph()
kpn._overview()
kpn._calculate_graph_overview()
kpn._jaaroverzicht()

client_name = "dfn"
dfn = DFNPickleETL(client=client_name, config=config.client_config[client_name])
dfn.extract()
dfn.transform()
dfn.analyse()
dfn.load()

# %% Test jaaroverzciht dfn

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ''
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
dfn = DFNTestETL(client='dfn', config=config.client_config['dfn'])
dfn = DFNETL(client='dfn', config=config.client_config['dfn'])
dfn.perform()
# %%
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ''
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
kpn = KPNETL(client='kpn', config=config.client_config['kpn'])
kpn.perform()
