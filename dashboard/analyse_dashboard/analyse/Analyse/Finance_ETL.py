"""
Finance_ETL.py
============

The ETL process for FttX Finance analyse.
"""
import copy

from sqlalchemy import bindparam, text

from Analyse.ETL import Extract, ETL, Transform, Load, ETLBase, logger
import pandas as pd

from Analyse.Record.RecordList import RecordList
from Analyse.Indicators.FinanceIndicator import FinanceIndicator
from functions import get_database_engine


class FinanceExtract(Extract):
    """
    Extracts data that is necesarry for the financial analyses
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.projects = self.config["projects"]
        self.client = self.config.get("name")

    def extract(self):
        """
        Extracts the data for every project for the budget, the realisation and the categorisation to group
        certain costs.
        """
        self.extracted_data.baan_budget = self._extract_sql_table('baan_budget')
        self.extracted_data.baan_realisation = self._extract_sql_table('baan_realisation')
        self._extract_categorisering()

    def _extract_sql_table(self, table):
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
            LEFT JOIN fc_baan_project_nr_name_map fcm
            ON baan.project = fcm.baan_nummer
            WHERE fcm.project_naam in :projects
            """  # nosec
        ).bindparams(bindparam("projects", expanding=True))
        df = pd.read_sql(sql, get_database_engine(), params={"projects": tuple(self.projects)})
        projects_category = pd.CategoricalDtype(categories=self.projects)
        df.drop_duplicates(inplace=True)
        df["project_naam"] = df['project_naam'].astype(projects_category)
        return df

    def _extract_categorisering(self):
        """
        Extracts the categorisation from the sql database

        Returns: pd.DataFrame containing the categorisation

        """

        logger.info("Extracting categorisation from the sql database")
        df = pd.read_excel(self.config['categorisation'])
        self.extracted_data.categorisation = df


class FinanceTransform(Transform):
    """
    Transforms extracted data that is necesarry for the financial analyses
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def transform(self):
        """
        Main tranform function that transfroms the extracted data and assigns it to
        transformed_data
        """
        super().transform()
        self._transform_categorisering()
        self._transform_baan_budget()
        self._transform_baan_realisation()

    def _transform_categorisering(self):
        """
        Funtion that transforms the Categorisation table and assigns the right datatype
        """
        logger.info('Transforming categorisation ...')
        df = copy.deepcopy(self.extracted_data.categorisation)
        df.rename(columns={'artikelcode': 'kostendrager'}, inplace=True)
        df.kostendrager = df.kostendrager.str.strip()
        df[['categorie', 'sub_categorie']] = df[['categorie', 'sub_categorie']].apply(lambda x: x.str.strip())
        df[['categorie', 'sub_categorie']] = df[['categorie', 'sub_categorie']].apply(lambda x: x.str.lower())
        self.transformed_data.categorisation = df

    def _transform_baan_budget(self):
        """
        Funtion that transforms the Baan budget table, add categorisation and assigns the right datatype
        """
        logger.info('Transforming Baan Budget ...')
        df = copy.deepcopy(self.extracted_data.baan_budget)
        df.bedrag = df.bedrag.astype(float)
        df = self._add_categorisation_to_baan_tables(df)
        self.transformed_data.baan_budget = df

    def _transform_baan_realisation(self):
        """
        Funtion that transforms the Baan realisation table, add categorisation and assigns the right datatype
        """
        logger.info('Transforming Baan Realisation ...')
        df = copy.deepcopy(self.extracted_data.baan_realisation)
        df = self._remove_income_from_realisation(df)
        df = self._transform_column_bedrag(df)
        df = self._add_categorisation_to_baan_tables(df)
        df = self._fix_dates(df)
        self.transformed_data.baan_realisation = df

    def _add_categorisation_to_baan_tables(self, df):
        """
        Function to merge categorisation on the Baan tables
        Args:
            df: pd.DataFrame containing data

        Returns: pd.DataFrame with columns for categorisation

        """
        df = df.merge(self.transformed_data.categorisation, on='kostendrager', how='left')
        df.sub_categorie.fillna('no_sub_category', inplace=True)
        df.categorie.fillna('no_category', inplace=True)
        return df

    def _remove_income_from_realisation(self, df):
        """
        function to remove kostensoort 'Opbrengsten' from the dataframe
        Args:
            df: pd.DataFrame with data of Baan

        Returns: pd.DataFrame without 'opbrengsten'

        """
        df.kostensoort = df.kostensoort.str.strip()
        mask = (df.kostensoort != 'Opbrengsten')
        return df[mask]

    def _transform_column_bedrag(self, df):
        """
        Function to transform the 'bedrag' column into a postive float
        Args:
            df: pd.DataFrame with data of Baan

        Returns: pd.DataFrame with adjusted column 'bedrag'
        """
        df.bedrag = df.bedrag.astype(float) * -1
        return df

    def _fix_dates(self, df):
        """
        Function to force date columns to datetime
        Args:
            df: pd.DataFrame including date columns

        Returns: pd.DataFrame with converted datetime columns

        """
        date_columns = ['vastlegdatum']
        df[date_columns] = df[date_columns].apply(pd.to_datetime, infer_datetime_format=True, errors="coerce", utc=True)
        for col in date_columns:
            df[col] = df[col].dt.strftime("%Y-%m-%d")
        return df


class FinanceAnalyse(ETLBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not hasattr(self, "config"):
            self.config = kwargs.get("config")
        self.records = RecordList()

    def analyse(self):
        """
        Main finance analyse that executes the FinanceIndicator and adds the records to self.records
        """
        logger.info(f"Analyse Finance for {self.config.get('name')}...")
        self.records.append(
            FinanceIndicator(client=self.client,
                             df_budget=self.transformed_data.baan_budget,
                             df_actuals=self.transformed_data.baan_realisation).perform()
        )


class FinanceLoad(Load):
    """
    Main finance load function that loads the self.records to the firestore
    """

    def load(self):
        logger.info("Loading documents...")
        self.records.to_firestore()


class FinanceETL(ETL, FinanceExtract, FinanceTransform, FinanceAnalyse, FinanceLoad):
    """
    Main finance ETL process with the perform function that executes all the ETL steps
    """

    def perform(self):
        self.extract()
        self.transform()
        self.analyse()
        self.load()
