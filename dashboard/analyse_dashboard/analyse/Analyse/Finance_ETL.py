"""
Finance_ETL.py
============

The ETL process for FttX Finance analyse.
"""
from sqlalchemy import bindparam, text

from Analyse.ETL import Extract, ETL, Transform, Load, ETLBase, logger
import pandas as pd

from Analyse.Record.RecordList import RecordList
from functions import get_database_engine


class FinanceExtract(Extract):
    """
    Extracts data that is necesarry for the financial analyses
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.projects = self.config["projects"]
        self.client_name = self.config.get("name")

    def extract(self):
        """
        Extracts the data for every project for the budget, the realisation and the categorisation to group
        certain costs.
        """
        super().extract()
        self.extracted_data.baan_budget = self.extract_sql_table('baan_budget')
        self.extracted_data.baan_realisation = self.extract_sql_table('baan_realisation')
        self.extract_categorisering()

    def extract_sql_table(self, table):
        """
        Extracts sql table
        Args:
            table (str): table to extract

        Returns: pd.DataFrame containing the data of the table

        """

        logger.info(f"Extracting {table} from the sql database")
        sql = text(
            f"""
            SELECT baan.*, fcm.project_naam
            FROM {table} baan
            INNER JOIN fc_baan_project_nr_name_map fcm
            ON baan.project = fcm.baan_nummer
            WHERE fcm.project_naam in :projects
            """  # nosec
        ).bindparams(bindparam("projects", expanding=True))
        df = pd.read_sql(sql, get_database_engine(), params={"table": table, "projects": tuple(self.projects)})
        projects_category = pd.CategoricalDtype(categories=self.projects)
        df["project_naam"] = df['project_naam'].astype(projects_category)
        return df

    def extract_categorisering(self):
        """
        Extracts the categorisation from the sql database

        Returns: pd.DataFrame containing the categorisation

        """

        logger.info("Extracting categorisation from the sql database")
        df = pd.read_excel(self.config['categorisation'], index=False)
        self.extracted_data.categorisation = df


class FinanceTransform(Transform):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def transform(self):
        super().transform()
        self.transform_baan_budget()
        self.transform_baan_realisation()
        self.transform_categorisering()

    def transform_baan_budget(self):
        self.transformed_data.baan_budget = pd.DataFrame()

    def transform_baan_realisation(self):
        self.transformed_data.baan_realisation = pd.DataFrame()

    def transform_categorisering(self):
        self.transformed_data.categorisation = pd.DataFrame()


class FinanceAnalyse(ETLBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not hasattr(self, "config"):
            self.config = kwargs.get("config")
        self.records = RecordList()

    def analyse(self):
        """
        Returns: Indicator
        """
        logger.info(f"Analyse Finance for {self.config.get('name')}...")
        ...


class FinanceLoad(Load):

    def load(self):
        logger.info("Loading documents...")
        self.records.to_firestore()


class FinanceETL(ETL, FinanceExtract, FinanceTransform, FinanceAnalyse, FinanceLoad):

    def perform(self):
        self.extract()
        self.transform()
        self.analyse()
        self.load()
