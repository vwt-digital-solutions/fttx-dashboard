"""
FttX.py
============

The ETL process for FttX. It contains all steps that are common for all FttX clients.
"""

import logging
import pickle  # nosec
from datetime import timedelta

import pandas as pd
from google.cloud import firestore
from pandas.api.types import CategoricalDtype
from sqlalchemy import bindparam, text

import business_rules as br
from Analyse.Data import Data
from Analyse.ETL import ETLBase, Extract, Load, Transform
from Analyse.Record.RecordListWrapper import RecordListWrapper
from config import FC_HISTORY_TABLE
from functions import cluster_reden_na, get_database_engine
from toggles import ReleaseToggles

logger = logging.getLogger("FttX Analyse")

toggles = ReleaseToggles("toggles.yaml")


class FttXBase(ETLBase):
    """
    The Base class for FttX. It collects the client and config in the __init__ and sets up the self.records and the
    self.intermediate_results

    Args:
        **kwargs: Keyword arguments that should contain the client and config.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = kwargs.get("client", "client_unknown")
        self.records = RecordListWrapper(client=self.client)
        self.intermediate_results = Data()


class FttXExtract(Extract):
    """
    Extracts data that is relevant for all FttX clients.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.projects = self.config["projects"]
        self.client_name = self.config.get("name")
        self.history_table = FC_HISTORY_TABLE

    # TODO: Documentation by Erik van Egmond
    def extract(self):
        """
        Extracts all data from the projects catalog for the projects set.

        Sets datasets on self.extracted_data.
        """
        logger.info("Extracting the aansluitingen")
        self._extract_from_sql()
        self._append_history()
        self.extract_project_info()

    # TODO: Documentation by Erik van Egmond
    def _extract_from_sql(self):
        logger.info("Extracting from the sql database")
        sql = text(
            """
select *
from fc_aansluitingen fca
where project in :projects
"""
        ).bindparams(
            bindparam("projects", expanding=True)
        )  # nosec
        df = pd.read_sql(
            sql, get_database_engine(), params={"projects": tuple(self.projects)}
        )
        projects_category = pd.CategoricalDtype(categories=self.projects)
        df["project"] = df.project.astype(projects_category)
        self.extracted_data.df = df

    def _append_history(self):
        """
        Main function to extra data from history table in the database
        and append this to the self.extracted_data.df
        """

        sql_engine = get_database_engine()
        self._extract_first_changedate_opleverdatum(sql_engine)
        self._extract_laswerkgereed_datum(sql_engine, column="laswerkapgereed")
        self._extract_laswerkgereed_datum(sql_engine, column="laswerkdpgereed")
        self._extract_status_civiel_datum(sql_engine)
        self._has_last_change_date(sql_engine)

    def _extract_first_changedate_opleverdatum(self, sql_engine):
        """
        Function to extract the date of the first time a `opleverdatum` was filled.
        """

        logger.info('Extracting history "Opleverdatum" from sql database')
        sql = text(
            f"""
SELECT fc.sleutel, MIN(fc.creationDate) AS 'opleverdatum'
FROM {self.history_table} as fc
WHERE fc.variable = 'opleverdatum'
AND `project` IN :projects
GROUP BY `sleutel`
"""  # nosec
        ).bindparams(bindparam("projects", expanding=True))
        df = pd.read_sql(
            sql, sql_engine.connect(), params={"projects": tuple(self.projects)}
        )

        # rename old column for 'opleverdatum' and then merge new opleverdatum from history.
        self.extracted_data.df = self.extracted_data.df.rename(
            columns={"opleverdatum": "opleverdatum_old"}
        )
        self.extracted_data.df = self.extracted_data.df.merge(
            df, how="left", on="sleutel"
        )

        cols = ["opleverdatum", "opleverdatum_old"]
        self.extracted_data.df[cols] = self.extracted_data.df[cols].apply(
            pd.to_datetime, infer_datetime_format=True, errors="coerce"
        )

        # correct 1 day delay in data delivery of robot. So creationDate is officially 1 day to late.
        self.extracted_data.df["opleverdatum"] = self.extracted_data.df[
            "opleverdatum"
        ] - timedelta(days=1)

        # building-up history began 2020-10-05, before that day use the opleverdatum as_is
        # to prevent large peaks in timeseries
        mask = self.extracted_data.df["opleverdatum_old"] < pd.Timestamp("2020-10-05")
        self.extracted_data.df.loc[mask, "opleverdatum"] = self.extracted_data.df.loc[
            mask, "opleverdatum_old"
        ]
        self.extracted_data.df.drop(columns=["opleverdatum_old"], inplace=True)

    def _extract_laswerkgereed_datum(self, sql_engine, column):
        """Function to extract the laswerk gereed datum from the history table in the database"""

        logger.info(f'Extracting history "{column}" from sql database')
        column_name = f"{column}_datum"

        sql = text(
            f"""
SELECT sleutel, MIN(`creationDate`)
AS '{column_name}'
FROM {self.history_table}
WHERE `variable` = '{column}'
AND `value` = '1'
AND `project` IN :projects
GROUP BY `sleutel`
"""
        ).bindparams(
            bindparam("projects", expanding=True)
        )  # nosec
        df = pd.read_sql(
            sql, sql_engine.connect(), params={"projects": tuple(self.projects)}
        )

        # correct 1 day delay in data delivery of robot. So creationDate is officially 1 day to late.
        df[column_name] = df[column_name].apply(
            pd.to_datetime, infer_datetime_format=True, errors="coerce"
        )
        df[column_name] = df[column_name] - timedelta(days=1)

        self.extracted_data.df = self.extracted_data.df.merge(
            df, how="left", on="sleutel"
        )

    def _extract_status_civiel_datum(self, sql_engine):
        """Function to extract the status_civiel datum from the history table in the database"""

        logger.info('Extracting history "status_civiel" from sql database')
        sql = text(
            f"""
SELECT sleutel, MIN(`creationDate`)
AS 'status_civiel_datum'
FROM {self.history_table}
WHERE `variable` = 'status_civiel'
AND `value` NOT LIKE '0%'
AND `project` IN :projects
GROUP BY `sleutel`
"""  # nosec
        ).bindparams(bindparam("projects", expanding=True))
        df = pd.read_sql(
            sql, sql_engine.connect(), params={"projects": tuple(self.projects)}
        )

        # correct 1 day delay in data delivery of robot. So creationDate is officially 1 day to late.
        df["status_civiel_datum"] = df["status_civiel_datum"].apply(
            pd.to_datetime, infer_datetime_format=True, errors="coerce"
        )
        df["status_civiel_datum"] = df["status_civiel_datum"] - timedelta(days=1)

        self.extracted_data.df = self.extracted_data.df.merge(
            df, how="left", on="sleutel"
        )

    def _has_last_change_date(self, sql_engine):
        """Function to extract the last_change datum of hasdatum from the history table in the database"""

        logger.info('Extracting history "hasdatum" from sql database')
        sql = text(
            f"""
SELECT sleutel, MAX(`creationDate`)
AS 'hasdatum_change_date'
FROM {self.history_table}
WHERE `variable` = 'hasdatum'
AND `project` IN :projects
GROUP BY `sleutel`
"""  # nosec
        ).bindparams(bindparam("projects", expanding=True))
        df = pd.read_sql(
            sql, sql_engine.connect(), params={"projects": tuple(self.projects)}
        )

        # correct 1 day delay in data delivery of robot. So creationDate is officially 1 day to late.
        df["hasdatum_change_date"] = df["hasdatum_change_date"].apply(
            pd.to_datetime, infer_datetime_format=True, errors="coerce"
        )
        df["hasdatum_change_date"] = df["hasdatum_change_date"] - timedelta(days=1)

        self.extracted_data.df = self.extracted_data.df.merge(
            df, how="left", on="sleutel"
        )

    def extract_project_info(self):
        """
        Extracts project information for all projects of a client. Project information contains
        FTU dates, Civiel start dates, total meters of tuinschieten, total meters of bis, total number of
        houses and desired speed in meter per week.

        Sets self.extracted_data:

        -   ftu: as dict with keys [date_FTU0, date_FTU1]
        -   civiel_startdatum: dict with project as key and startdate as value
        -   total_meters_tuinschieten: dict with project as key and meters as value
        -   total_meters_bis: dict with project as key and meters as value
        -   total_number_huisaansluitingen: dict with project as key and number as value
        -   snelheid_mpw: with project as key and speed as value
        -   info_per_project: dict with project as key and all of the above information as value
        """

        logger.info(f"Extracting FTU {self.client_name}")
        doc = (
            firestore.Client()
            .collection("ProjectInfo")
            .document(f"{self.client_name}_project_dates")
            .get()
            .to_dict()
            .get("record")
        )

        self.extracted_data.ftu = Data(
            {"date_FTU0": doc["FTU0"], "date_FTU1": doc["FTU1"]}
        )
        self.extracted_data.civiel_startdatum = doc.get("Civiel startdatum")
        self.extracted_data.total_meters_tuinschieten = doc.get("meters tuinschieten")
        self.extracted_data.total_meters_bis = doc.get("meters BIS")
        self.extracted_data.total_number_huisaansluitingen = doc.get(
            "huisaansluitingen"
        )
        self.extracted_data.snelheid_mpw = doc.get("snelheid (m/week)")

        # from_dict sets none values as np.nan
        self.extracted_data.project_info = (
            pd.DataFrame.from_dict(doc, orient="columns")
            .fillna(999)
            .replace({999: None})
            .to_dict(orient="index")
        )


# TODO: Documentation by Erik van Egmond
class PickleExtract(Extract, FttXBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    # TODO: Documentation by Erik van Egmond
    def extract(self):
        logger.info("Extracting data, trying to use a pickle")
        pickle_name = f"{self.client}_data.pickle"
        try:
            self.extracted_data = pickle.load(open(pickle_name, "rb"))  # nosec
            logger.info("Extracted data from pickle")
        except (OSError, IOError, FileNotFoundError):
            logger.info(
                f"{pickle_name} not available, using fallback and pickling the result"
            )
            super().extract()
            pickle.dump(self.extracted_data, open(pickle_name, "wb"))


# TODO: Documentation by Erik van Egmond
class FttXTransform(Transform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.year = kwargs.get("year", str(pd.Timestamp.now().year))

    # TODO: Documentation by Erik van Egmond
    def transform(self, **kwargs):
        super().transform()
        logger.info("Transforming the data following the FttX protocol")
        self._fix_dates()
        self._cluster_reden_na()

    def _is_ftu_available(self, project):
        """
        This functions checks whether a FTU0 date is available

        Args:
            project: the project name

        Returns:
            bool: boolean if ftu0 is available or not

        """
        available = False
        ftu0 = self.transformed_data.ftu["date_FTU0"].get(project)
        if ftu0:
            available = True
        return available

    def _make_project_list(self):
        """
        This functions returns a list of projects that have at least a FTU0 date.
        All the projects in this list will be evaluated in the analysis.

        Returns:
            list: returns a list of projects names
        """
        project_list = []
        if self.client == "tmobile":
            self.project_list = self.config["projects"]
        else:
            for project in self.config["projects"]:
                if self._is_ftu_available(project):
                    project_list.append(project)
                else:
                    logger.info(f"For the {project} we do not have a FTU0 date")
            self.project_list = project_list

    # TODO: Documentation by Erik van Egmond
    def _fix_dates(self):
        """Function that tranfsorms the columns with a date to pd.datetime format"""

        logger.info("Transforming columns to datetime format")
        self.transformed_data.datums = datums = [
            "activatie_datum",
            "hasdatum",
            "laswerkapgereed_datum",
            "laswerkdpgereed_datum",
            "opleverdatum",
            "plandatum",
            "schouwdatum",
            "status_civiel_datum",
            "toestemming_datum",
            "creation",
            "plan_date",
            "hasdatum_change_date",
        ]
        self.transformed_data.df[datums] = self.transformed_data.df[datums].apply(
            pd.to_datetime, infer_datetime_format=True, errors="coerce", utc=True
        )

        self.transformed_data.df[datums] = self.transformed_data.df[datums].apply(
            lambda x: x.dt.tz_convert(None)
        )
        for col in datums:
            self.transformed_data.df[col] = pd.to_datetime(
                self.transformed_data.df[col].dt.strftime("%Y-%m-%d")
            )

    def _cluster_reden_na(self):
        """
        Add cluster redenna column to transformed data df

        Returns:

        """
        logger.info("Transforming dataframe through adding column cluster redenna")
        clus = self.config["clusters_reden_na"]
        self.transformed_data.df.loc[:, "cluster_redenna"] = self.transformed_data.df[
            "redenna"
        ].apply(lambda x: cluster_reden_na(x, clus))
        self.transformed_data.df.loc[
            br.hc_opgeleverd(self.transformed_data.df), ["cluster_redenna"]
        ] = "HC"
        cluster_types = CategoricalDtype(categories=list(clus.keys()), ordered=True)
        self.transformed_data.df["cluster_redenna"] = self.transformed_data.df[
            "cluster_redenna"
        ].astype(cluster_types)


class FttXLoad(Load, FttXBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    # TODO: Documentation by Erik van Egmond
    def load(self):
        logger.info("Loading documents...")
        self.records.to_firestore()

    # TODO: Documentation by Erik van Egmond


class FttXTestLoad(FttXLoad):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    # TODO: Documentation by Erik van Egmond
    def load(self):
        logger.info("Nothing is loaded to the firestore as this is a test")
        logger.info("The following documents would have been updated/set:")
        for document in self.records:
            logger.info(document.document_name())

    # TODO: Documentation by Erik van Egmond
