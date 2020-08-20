from analyse.ETL import ETL_project_data_database, ETL_planning_data, ETL_target_data, ETL_project_data


class Customer():
    def __init__(self, config):
        for key, value in config.items():
            setattr(self, key, value)
        self.etl_set = False

    def _set_etl(self):
        if not self.etl_set:
            self.set_etl_processes()


# Moved decorator out of class scope, does seem to be the pythonic way to do it.
def etl(function):
    def wrapper(self):
        self._set_etl()
        func = function(self)
        return func
    return wrapper


class CustomerKPN(Customer):

    def set_etl_processes(self):
        self.etl_project_data = ETL_project_data_database
        self.etl_planning_data = ETL_planning_data
        self.etl_target_data = ETL_target_data
        self.etl_set = True

    @etl
    def get_data(self):
        etl = self.etl_project_data(self.bucket, self.projects, self.columns)
        etl.extract()
        etl.transform()
        return etl.data

    @etl
    def get_data_planning(self):
        etl = self.etl_planning_data(self.planning_location)
        etl.extract()
        etl.transform()
        return etl.data

    @etl
    def get_data_targets(self):
        etl = self.etl_target_data(self.target_document)
        etl.extract()
        etl.transform()
        return self.etl.FTU0, etl.FTU1


class CustomerTmobile(Customer):

    def set_etl_processes(self):
        self.etl_project_data = ETL_project_data
        self.etl_set = True

    @etl
    def get_data(self, local_file=None):
        etl = self.etl_project_data(self.bucket, self.projects, self.columns)
        etl.extract(local_file)
        etl.transform()
        return etl.data
