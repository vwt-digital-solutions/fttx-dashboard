"""
Finance_ETL.py
============

The ETL process for FttX Finance analyse.
"""
from Analyse.ETL import Extract, ETL, Transform, Load, ETLBase, logger
import pandas as pd

from Analyse.Record.RecordList import RecordList


class FinanceExtract(Extract):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def extract(self):
        super().extract()
        self.extract_baan_budget_from_sql()
        self.extract_baan_realisation_from_sql()
        self.extract_categorisering()

    def extract_baan_budget_from_sql(self):
        self.extracted_data.baan_budget = pd.DataFrame()

    def extract_baan_realisation_from_sql(self):
        self.extracted_data.baan_realisation = pd.DataFrame()

    def extract_categorisering(self):
        self.extracted_data.categorisation = pd.DataFrame()


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
