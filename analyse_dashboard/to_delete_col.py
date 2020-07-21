from analyse.functions import empty_collection
import analyse.config as config
import os

keys = os.listdir(config.path_jsons)
for fn in keys:
    if ('-d-' in fn) & ('-fttx-' in fn):
        gpath_d = config.path_jsons + fn
    if '-p-' in fn:
        gpath_p = config.path_jsons + fn
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gpath_d

empty_collection(config.subset_KPN_2020)
