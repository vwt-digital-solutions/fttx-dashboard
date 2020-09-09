import os
try:
    from analyse.ETL import (
        ExtractTransformProjectDataFirestoreToDfList,
        ExtractTransformPlanningData,
        ExtractTransformTargetData,
        ExtractTransformProjectDataFirestore
    )
    import analyse.config as config
except ImportError as e:
    print(e)
    from ETL import (
        ExtractTransformProjectDataFirestoreToDfList,
        ExtractTransformPlanningData,
        ExtractTransformTargetData,
        ExtractTransformProjectDataFirestore
    )
    import config


class Customer:
    def __init__(self, config):
        self.config = config

    def __str__(self):
        return f"{self.__class__.__qualname__}\nconfig:\n{self.config}"


class CustomerKPN(Customer):
    def get_data(self):
        etl = ExtractTransformProjectDataFirestoreToDfList(
            self.config["bucket"], self.config["projects"], self.config["columns"]
        )
        return etl.data

    def get_data_planning(self):
        etl = ExtractTransformPlanningData(self.config["planning_location"])
        return etl.data

    def get_data_targets(self):
        etl = ExtractTransformTargetData(self.config["target_location"])
        return etl.date_FTU0, etl.date_FTU1


class CustomerTmobile(Customer):
    def get_data(self):
        etl = ExtractTransformProjectDataFirestore(
            self.config["bucket"],
            self.config["projects"],
            self.config["columns"],
        )
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
