try:
    from analyse.ETL import (
        ExtractTransformProjectDataFirestoreToDfList,
        ExtractTransformPlanningData,
        ExtractTransformTargetData,
        ExtractTransformProjectDataFirestore
    )
except ImportError as e:
    print(e)
    from ETL import (
        ExtractTransformProjectDataFirestoreToDfList,
        ExtractTransformPlanningData,
        ExtractTransformTargetData,
        ExtractTransformProjectDataFirestore
    )


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
        etl = ExtractTransformTargetData()
        return etl.data["FTU0"], etl.data["FTU1"]


class CustomerTmobile(Customer):
    def get_data(self):
        etl = ExtractTransformProjectDataFirestore(
            self.config["bucket"],
            self.config["projects"],
            self.config["columns"],
        )
        return etl.data
