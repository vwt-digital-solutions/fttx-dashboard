try:
    from analyse.ETL import ExtractTransformProjectDataDatabase, ExtractTransformPlanningData, ExtractTransformTargetData, \
        ExtractTransformProjectData
    import analyse.config as config
except ImportError:
    from ETL import ExtractTransformProjectDataDatabase, ExtractTransformPlanningData, ExtractTransformTargetData, \
        ExtractTransformProjectData
    import config
import os


class Customer:
    def __init__(self, config):
        self.config = config


class CustomerKPN(Customer):

    def get_data(self):
        etl = ExtractTransformProjectDataDatabase(self.config["bucket"], self.config["projects"],
                                                  self.config["columns"])
        return etl.data

    def get_data_planning(self):
        etl = ExtractTransformPlanningData(self.config["local_location"])
        return etl.data

    def get_data_targets(self):
        etl = ExtractTransformTargetData(self.config["local_location"])
        return etl.date_FTU0, etl.date_FTU1


class CustomerTmobile(Customer):

    def get_data(self, local_file=None):
        etl = ExtractTransformProjectData(self.config["bucket"], self.config["projects"], self.config["columns"],
                                          run=False)
        etl.extract(local_file)
        etl.transform()
        return etl.data


def get_key(env):
    keys = os.listdir(config.path_jsons)
    for fn in keys:
        if ('-d-' in fn) & ('-fttx-' in fn):
            gpath_d = config.path_jsons + fn
        if ('-p-' in fn) & ('-fttx-' in fn):
            gpath_p = config.path_jsons + fn
        if ('-d-' in fn) & ('-it-fiber' in fn):
            gpath_i = config.path_jsons + fn

    if env == 'dev':
        gpath = gpath_d
    if env == 'prd':
        gpath = gpath_p
    if env == 'fc':
        gpath = gpath_i
    print(gpath)
    return gpath
