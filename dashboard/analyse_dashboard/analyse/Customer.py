try:
    from analyse.ETL import ExtractTransformProjectDataDatabase, ExtractTransformPlanningData, ExtractTransformTargetData, \
        ExtractTransformProjectData
except ImportError:
    from ETL import ExtractTransformProjectDataDatabase, ExtractTransformPlanningData, ExtractTransformTargetData, \
        ExtractTransformProjectData


class Customer:
    def __init__(self, config):
        self.config = config


class CustomerKPN(Customer):

    def get_data(self):
        etl = ExtractTransformProjectDataDatabase(self.config["bucket"], self.config["projects"],
                                                  self.config["columns"])
        return etl.data

    def get_data_planning(self):
        etl = ExtractTransformPlanningData(self.config["planning_location"])
        return etl.data

    def get_data_targets(self):
        etl = ExtractTransformTargetData()
        return etl.data['FTU0'], etl.data['FTU1']


class CustomerTmobile(Customer):

    def get_data(self, local_file=None):
        etl = ExtractTransformProjectData(self.config["bucket"], self.config["projects"], self.config["columns"],
                                          run=False)
        etl.extract(local_file)
        etl.transform()
        return etl.data
