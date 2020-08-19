from analyse.ETL import ETL_project_data_database, ETL_planning_data, ETL_target_data, ETL_project_data


class Customer():

    def __init__(self, config):
        for key, value in config.items():
            setattr(self, key, value)

    def set_etl_processes(self):
        self.etl_project_data = ETL_project_data_database
        self.etl_planning_data = ETL_planning_data
        self.etl_target_data = ETL_target_data

    def get_data(self):
        etl = self.etl_project_data(self.bucket, self.projects, self.columns)
        etl.extract()
        etl.transform()
        return etl.data

    def get_data_planning(self):
        etl = self.etl_planning_data(self.planning_location)
        etl.extract()
        etl.transform()
        return etl.data

    def get_data_targets(self):
        etl = self.etl_target_data(self.target_document)
        etl.extract()
        etl.transform()
        self. etl.FTU0, etl.FTU1

    def get_source_data(self):
        self.set_etl_processes()
        self.get_data()
        self.get_data_planning()
        self.get_data_targets()


class Customer_tmobile(Customer):

    def set_etl_processes(self):
        self.etl_project_data = ETL_project_data

    def get_data(self, local_file=None):
        etl = self.etl_project_data(self.bucket, self.projects, self.columns)
        etl.extract(local_file)
        etl.transform()
        return etl.data
