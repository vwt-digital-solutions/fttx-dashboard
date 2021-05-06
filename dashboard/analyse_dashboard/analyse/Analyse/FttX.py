"""
FttX.py
============

The ETL process for FttX. It contains all steps that are common for all FttX clients.
"""

import logging
import os
import pickle  # nosec
from datetime import timedelta

import numpy as np
import pandas as pd
from google.cloud import firestore
from pandas.api.types import CategoricalDtype
from sqlalchemy import bindparam, text

import business_rules as br
from Analyse.Data import Data
from Analyse.ETL import ETL, ETLBase, Extract, Load, Transform
from Analyse.Record.DictRecord import DictRecord
from Analyse.Record.DocumentListRecord import DocumentListRecord
from Analyse.Record.ListRecord import ListRecord
from Analyse.Record.Record import Record
from Analyse.Record.RecordListWrapper import RecordListWrapper
from functions import (
    calculate_current_werkvoorraad, calculate_redenna_per_period,
    cluster_reden_na, create_project_filter, extract_aangesloten_orders_dates,
    extract_bis_target_client, extract_bis_target_overview,
    extract_has_target_client, extract_planning_dates,
    extract_realisatie_hc_dates, extract_realisatie_hpend_dates,
    extract_target_dates, extract_voorspelling_dates,
    extract_werkvoorraad_has_dates, get_database_engine, individual_reden_na,
    overview_reden_na, ratio_sum_over_periods_to_record, rules_to_state,
    sum_over_period_to_record,
    voorspel_and_planning_minus_HPend_sum_over_periods_to_record)
from toggles import ReleaseToggles
from config import FC_HISTORY_TABLE

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
        logger.info("Extracting the Projects collection")
        self._extract_from_sql()
        self._append_history()
        self.extract_project_info()
        if toggles.leverbetrouwbaarheid:
            self._extract_leverbetrouwbaarheid_dataframe()

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
        self._extract_laswerkgereed_datum(sql_engine, column='laswerkapgereed')
        self._extract_laswerkgereed_datum(sql_engine, column='laswerkdpgereed')
        self._extract_status_civiel_datum(sql_engine)
        self._has_last_change_date(sql_engine)

    def _extract_first_changedate_opleverdatum(self, sql_engine):
        """
        Function to extract the date of the first time a `opleverdatum` was filled
        from the history table in the database.
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
        ).bindparams(
            bindparam("projects", expanding=True)
        )
        df = pd.read_sql(
            sql, sql_engine.connect(), params={"projects": tuple(self.projects)}
        )

        # rename old column for 'opleverdatum' and then merge new opleverdatum from history.
        self.extracted_data.df = self.extracted_data.df.rename(columns={'opleverdatum': 'opleverdatum_old'})
        self.extracted_data.df = self.extracted_data.df.merge(df, how='left', on='sleutel')

        cols = ['opleverdatum', 'opleverdatum_old']
        self.extracted_data.df[cols] = self.extracted_data.df[cols].apply(pd.to_datetime,
                                                                          infer_datetime_format=True,
                                                                          errors="coerce")

        # correct 1 day delay in data delivery of robot. So creationDate is officially 1 day to late.
        self.extracted_data.df['opleverdatum'] = self.extracted_data.df['opleverdatum'] - timedelta(days=1)

        # building-up history began 2020-10-05, before that day use the opleverdatum as_is
        # to prevent large peaks in timeseries
        mask = self.extracted_data.df['opleverdatum_old'] < pd.Timestamp("2020-10-05")
        self.extracted_data.df.loc[mask, 'opleverdatum'] = self.extracted_data.df.loc[mask, 'opleverdatum_old']
        self.extracted_data.df.drop(columns=['opleverdatum_old'], inplace=True)

    def _extract_laswerkgereed_datum(self, sql_engine, column):
        """Function to extract the laswerk gereed datum from the history table in the database"""

        logger.info(f'Extracting history "{column}" from sql database')
        column_name = f'{column}_datum'

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
        self.extracted_data.df = self.extracted_data.df.merge(df, how='left', on='sleutel')

    def _extract_status_civiel_datum(self, sql_engine):
        """Function to extract the status_civiel datum from the history table in the database"""

        logger.info('Extracting history "status_civiel" from sql database')
        sql = text(
            f"""
SELECT sleutel, MIN(`creationDate`)
AS 'status_civiel_datum'
FROM {self.history_table}
WHERE `variable` = 'status_civiel'
AND `value` IN('1', '1 / Voor Gevel', '1 / Geul')
AND `project` IN :projects
GROUP BY `sleutel`
"""  # nosec
        ).bindparams(
            bindparam("projects", expanding=True)
        )
        df = pd.read_sql(
            sql, sql_engine.connect(), params={"projects": tuple(self.projects)}
        )
        self.extracted_data.df = self.extracted_data.df.merge(df, how='left', on='sleutel')

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
        ).bindparams(
            bindparam("projects", expanding=True)
        )
        df = pd.read_sql(
            sql, sql_engine.connect(), params={"projects": tuple(self.projects)}
        )
        self.extracted_data.df = self.extracted_data.df.merge(df, how='left', on='sleutel')

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

    def _extract_leverbetrouwbaarheid_dataframe(self):
        """
        This function extracts a pd.DataFrame from the transition log (fc_transitie_log) and the aansluitingen dataset
        (fc_aansluitingen) that contains houses of which: \n
        -   the hasdatum is equal to the opleverdatum.
        -   the hasdatum has been changed to the final hasdatum.
        -   the opleverdatum starts at 2021-01-01, to filter out a bunch of keys that were changed for the first time.
        This DataFrame can then be used to calculate the leverbetrouwbaarheid.
        """
        logger.info("Extracting dataframe for leverbetrouwbaarheid")
        sql = text(
            """
select  fctl.date as last_change_in_hasdatum,
        fctl.to_value as hasdatum_changed_to,
        fcas.hasdatum, fcas.opleverdatum, fcas.project
from fc_transitie_log as fctl
inner join fc_aansluitingen as fcas on
fctl.key = 'hasdatum' and
fctl.project in :projects and
fctl.sleutel = fcas.sleutel and
fctl.to_value = fcas.hasdatum and fcas.opleverdatum >= '2021-01-01'
"""
        ).bindparams(
            bindparam("projects", expanding=True)
        )  # nosec
        df = pd.read_sql(
            sql, get_database_engine(), params={"projects": tuple(self.projects)}
        )
        projects_category = pd.CategoricalDtype(categories=self.projects)
        df["project"] = df.project.astype(projects_category)
        self.extracted_data.leverbetrouwbaarheid = df


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
        self._make_project_list()
        self._fix_dates()
        self._cluster_reden_na()
        self._add_status_columns()
        self._set_totals()

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

    # TODO: Documentation by Mark Bruisten
    def _set_totals(self):
        """
        Sets total amount of connections per project in transformed_data dictionary.

        Returns:

        """
        self.transformed_data.totals = {}
        for project, project_df in self.transformed_data.df.groupby("project"):
            self.transformed_data.totals[project] = len(project_df)

    # TODO: Documentation by Erik van Egmond
    def _fix_dates(self):
        """Function that tranfsorms the columns with a date to pd.datetime format"""

        logger.info(
            "Transforming columns to datetime column if there is 'datum' in column name"
        )
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
            "hasdatum_change_date"
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

    # TODO: Documentation by Erik van Egmond
    def _add_status_columns(self):
        logger.info("Transforming dataframe through adding status columns")
        state_list = [
            "niet_opgeleverd",
            "ingeplanned",
            "opgeleverd_zonder_hc",
            "opgeleverd",
        ]
        self.transformed_data.df["false"] = False
        has_rules_list = [
            br.has_niet_opgeleverd(self.transformed_data.df),
            br.has_ingeplanned(self.transformed_data.df),
            br.hp_opgeleverd(self.transformed_data.df),
            br.hc_opgeleverd(self.transformed_data.df),
        ]
        logger.info("Added has_rules_list")
        has = rules_to_state(has_rules_list, state_list)
        geschouwd_rules_list = [
            ~br.toestemming_bekend(self.transformed_data.df),
            self.transformed_data.df["false"],
            self.transformed_data.df["false"],
            br.toestemming_bekend(self.transformed_data.df),
        ]
        geschouwd = rules_to_state(geschouwd_rules_list, state_list)
        logger.info("Added geschouwd_rules_list")

        bis_gereed_rules_list = [
            br.bis_niet_opgeleverd(self.transformed_data.df),
            self.transformed_data.df["false"],
            self.transformed_data.df["false"],
            br.bis_opgeleverd(self.transformed_data.df),
        ]
        bis_gereed = rules_to_state(bis_gereed_rules_list, state_list)
        logger.info("Added bis_gereed_rules_list")

        laswerkdpgereed_rules_list = [
            br.laswerk_dp_niet_gereed(self.transformed_data.df),
            self.transformed_data.df["false"],
            self.transformed_data.df["false"],
            br.laswerk_dp_gereed(self.transformed_data.df),
        ]
        laswerkdpgereed = rules_to_state(laswerkdpgereed_rules_list, state_list)

        logger.info("Added laswerkdpgereed_rules_list")

        laswerkapgereed_rules_list = [
            br.laswerk_ap_niet_gereed(self.transformed_data.df),
            self.transformed_data.df["false"],
            self.transformed_data.df["false"],
            br.laswerk_ap_gereed(self.transformed_data.df),
        ]
        laswerkapgereed = rules_to_state(laswerkapgereed_rules_list, state_list)

        logger.info("Added laswerkapgereed_rules_list")
        business_rules_list = [
            [geschouwd, "schouw_status"],
            [bis_gereed, "bis_status"],
            [self.transformed_data.df["soort_bouw"] == "Laag", "laagbouw"],
            [laswerkdpgereed, "lasDP_status"],
            [laswerkapgereed, "lasAP_status"],
            [has, "HAS_status"],
        ]
        neccesary_info_list = [
            [self.transformed_data.df["sleutel"], "sleutel"],
        ]

        series_list = business_rules_list + neccesary_info_list

        cols, colnames = list(zip(*series_list))
        status_df = pd.concat(cols, axis=1)
        status_df.columns = colnames
        self.transformed_data.df.drop("false", inplace=True, axis=1)
        self.transformed_data.df = pd.merge(
            self.transformed_data.df, status_df, on="sleutel", how="left"
        )


# TODO: Documentation by Erik van Egmond
class FttXAnalyse(FttXBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not hasattr(self, "config"):
            self.config = kwargs.get("config")
        self.records = RecordListWrapper(self.client)
        self.intermediate_results = Data()

    def analyse(self):
        """
        Main loop for FTTX analyse, runs a set of functions on transformed data that will result in a set of
        firestore records.

        """
        logger.info("Analysing using the FttX protocol")
        self._calculate_list_of_years()
        self._make_records_for_dashboard_values()
        self._make_records_of_client_targets_for_dashboard_values()
        self._make_records_of_voorspelling_and_planning_for_dashboard_values()
        if toggles.leverbetrouwbaarheid:
            self._make_records_of_ratios_for_dashboard_values()
        else:
            self._make_records_ratio_hc_hpend_for_dashboard_values()
            self._make_records_ratio_under_8weeks_for_dashboard_values()
        self._make_intermediate_results_ratios_project_specific_values()
        self._calculate_current_werkvoorraad()
        self._reden_na()
        self._set_filters()
        self._calculate_status_counts_per_project()
        self._calculate_redenna_per_period()
        self._progress_per_phase()
        self._progress_per_phase_over_time()

    def _progress_per_phase_over_time(self):
        """
        This function calculates the progress per phase over time base on the specified columns
        per phase:
            'opleverdatum': 'has',
            'schouwdatum': 'schouwen',
            'laswerkapgereed_datum': 'montage ap',
            'laswerkdpgereed_datum': 'montage dp',
            'status_civiel_datum': 'civiel'

        Adds a record consisting of dict per project holding a timeindex with progress per phase

        """
        logger.info("Calculating project progress per phase over time")
        document_list = []
        for project, df in self.transformed_data.df.groupby("project"):
            if df.empty:
                continue
            columns = [
                "opleverdatum",
                "schouwdatum",
                "laswerkapgereed_datum",
                "laswerkdpgereed_datum",
                "status_civiel_datum",
                "laswerkapgereed",
                "laswerkdpgereed",
            ]
            date_df = df.loc[:, columns]
            mask = br.laswerk_dp_gereed(df) & br.laswerk_ap_gereed(df)
            date_df["montage"] = np.datetime64("NaT")
            date_df.loc[mask, "montage"] = date_df[
                ["laswerkapgereed_datum", "laswerkdpgereed_datum"]
            ][mask].max(axis=1)
            date_df = date_df.drop(columns=["laswerkapgereed", "laswerkdpgereed"])
            progress_over_time: pd.DataFrame = date_df.apply(pd.value_counts).resample(
                "D"
            ).sum().cumsum() / len(df)
            progress_over_time.index = progress_over_time.index.strftime("%Y-%m-%d")
            progress_over_time.rename(
                columns={
                    "opleverdatum": "has",
                    "schouwdatum": "schouwen",
                    "laswerkapgereed_datum": "montage ap",
                    "laswerkdpgereed_datum": "montage dp",
                    "status_civiel_datum": "civiel",
                },
                inplace=True,
            )
            record = progress_over_time.to_dict()
            document_list.append(
                dict(
                    client=self.client,
                    project=project,
                    data_set="progress_over_time",
                    record=record,
                )
            )
        self.records.add(
            "Progress_over_time",
            document_list,
            DocumentListRecord,
            "Data",
            document_key=["client", "project", "data_set"],
        )

    def _progress_per_phase(self):
        """
        Calculates the progress per phase for the phases civiel, montage, schouwen, hc, hp and hp end, as well
        as the totals per project. These results are put in a record and added to the records attribute of the class.

        """
        logger.info("Calculating project progress per phase")

        progress_df = pd.concat(
            [
                self.transformed_data.df.project,
                ~self.transformed_data.df.sleutel.isna(),
                self.transformed_data.df.status_civiel.str.contains("1"),
                br.laswerk_dp_gereed(self.transformed_data.df)
                & br.laswerk_ap_gereed(self.transformed_data.df),
                br.geschouwed(self.transformed_data.df),
                br.hc_opgeleverd(self.transformed_data.df),
                br.hp_opgeleverd(self.transformed_data.df),
                br.opgeleverd(self.transformed_data.df),
            ],
            axis=1,
        )
        progress_df.columns = [
            "project",
            "totaal",
            "civiel",
            "montage",
            "schouwen",
            "hc",
            "hp",
            "hpend",
        ]
        documents = [
            dict(
                project=project, client=self.client, data_set="progress", record=values
            )
            for project, values in progress_df.groupby("project")
            .sum()
            .to_dict(orient="index")
            .items()
        ]

        self.records.add(
            "Progress",
            documents,
            DocumentListRecord,
            "Data",
            document_key=["client", "project", "data_set"],
        )

    def _calculate_list_of_years(self):
        """
        Calculates a list of years per client based on the dates that are found in the date columns.

        """
        logger.info("Calculating list of years")
        date_columns = self.transformed_data.datums
        dc_data = self.transformed_data.df.loc[:, date_columns]
        list_of_years = []
        for col in dc_data.columns:
            list_of_years += list(dc_data[col].dropna().dt.year.unique().astype(str))
        list_of_years = sorted(list(set(list_of_years)))

        self.records.add("List_of_years", list_of_years, Record, "Data")
        self.intermediate_results.List_of_years = list_of_years

    # TODO: Documentation by Erik van Egmond
    def _calculate_current_werkvoorraad(self):
        logger.info("Calculating y voorraad act for KPN")
        current_werkvoorraad = calculate_current_werkvoorraad(self.transformed_data.df)
        self.intermediate_results.current_werkvoorraad = current_werkvoorraad

    # TODO: Documentation by Casper van Houten
    def _reden_na(self):
        """
        Calculates records for reden na, reden_na_overview and reden_na_projects, and adds them to the records stored
        in the class.

        """
        logger.info("Calculating reden na graphs")
        overview_record = overview_reden_na(self.transformed_data.df)
        record_dict = individual_reden_na(self.transformed_data.df)
        self.records.add("reden_na_overview", overview_record, Record, "Data")
        self.records.add("reden_na_projects", record_dict, DictRecord, "Data")

    # TODO: Documentation by Casper van Houten
    def _set_filters(self):
        """
        Sets the set of projects that should be shown in the dashboard as record, so that it can be retrieved from the
        firestore.

        """
        self.records.add(
            "project_names",
            create_project_filter(self.transformed_data.df),
            ListRecord,
            "Data",
        )

    # TODO: Documentation by Erik van Egmond
    def _calculate_status_counts_per_project(self):
        logger.info("Calculating completed status counts per project")

        status_df: pd.DataFrame = self.transformed_data.df[
            [
                "schouw_status",
                "bis_status",
                "laagbouw",
                "lasDP_status",
                "lasAP_status",
                "HAS_status",
                "cluster_redenna",
                "sleutel",
                "project",
            ]
        ]
        status_df = status_df.rename(columns={"sleutel": "count"})
        status_counts_dict = {}
        col_names = list(status_df.columns)
        for project in status_df.project.unique():
            project_status = status_df[status_df.project == project][col_names[:-1]]
            status_counts_dict[project] = (
                project_status.groupby(col_names[:-2])
                .count()
                .reset_index()
                .dropna()
                .to_dict(orient="records")
            )
        self.records.add(
            "completed_status_counts", status_counts_dict, DictRecord, "Data"
        )

    def _calculate_redenna_per_period(self):
        """
        Calculates reden_na per periods for weeks, months, and years over all projects,
        based on hasdatum. Adds week, months and years all to sepearte records.

        """
        logger.info("Calculating redenna per period (week & month)")
        by_week = calculate_redenna_per_period(
            df=self.transformed_data.df, date_column="hasdatum", freq="W-MON"
        )
        self.records.add("redenna_by_week", by_week, Record, "Data")

        by_month = calculate_redenna_per_period(
            df=self.transformed_data.df, date_column="hasdatum", freq="M"
        )
        self.records.add("redenna_by_month", by_month, Record, "Data")

        by_year = calculate_redenna_per_period(
            df=self.transformed_data.df, date_column="hasdatum", freq="Y"
        )
        self.records.add("redenna_by_year", by_year, Record, "Data")

    def _make_records_for_dashboard_values(self):
        """
        Calculates the overzicht values per jaar of simple KPI's that do not contain a ratio or need a subtraction.
        These values are extracted as a pd.Series with dates, based on the underlying business rules (see the functions
        in function_dict). The values are then calculated per year (obtained from _calculate_list_of_years) and per
        period ('W-MON', 'M', 'Y') through the sum_over_period_to_record function. All these values are added as
        dictionaries to a document_list, which is added to the Firestore.

        """
        logger.info("Calculating records for dashboard overview values")
        df = self.transformed_data.df
        # Create a dictionary that contains the output name and the appropriate mask:
        function_dict = {
            "realisatie_bis": df[br.bis_opgeleverd(df)].status_civiel_datum,
            "werkvoorraad_has": extract_werkvoorraad_has_dates(df),
            "realisatie_hpend": extract_realisatie_hpend_dates(df),
            "target_intern_bis": extract_bis_target_overview(
                civiel_startdatum=self.transformed_data.get("civiel_startdatum"),
                total_meters_bis=self.transformed_data.get("total_meters_bis"),
                total_num_has=self.transformed_data.get(
                    "total_number_huisaansluitingen"
                ),
                snelheid_m_week=self.transformed_data.get("snelheid_mpw"),
                client=self.client,
            ),  # TODO: Remove when project info is available for tmobile and dfn
            "target": extract_target_dates(
                df=df,
                project_list=self.project_list,
                totals=self.transformed_data.get("totals"),
                ftu=self.extracted_data.get("ftu"),
            ),
            "voorspelling": extract_voorspelling_dates(
                df=df,
                ftu=self.extracted_data.get("ftu"),
                totals=self.transformed_data.get("totals"),
            ),
            "planning": extract_planning_dates(
                df=df,
                planning=self.transformed_data.get("planning"),
                client=self.client,
            ),
        }
        list_of_freq = ["W-MON", "M", "Y"]

        document_list = []
        for key, values in function_dict.items():
            for year in self.intermediate_results.List_of_years:
                for freq in list_of_freq:
                    record = sum_over_period_to_record(
                        timeseries=values, freq=freq, year=year
                    )
                    if len(record) == 1:  # removes the date when summing over a year
                        record = list(record.values())[0]
                    document_list.append(
                        dict(
                            client=self.client,
                            graph_name=key,  # output name from function_dict
                            frequency=freq,
                            year=year,
                            record=record,
                        )
                    )
        self.records.add(
            "Overzicht_per_jaar",
            document_list,
            DocumentListRecord,
            "Data",
            document_key=["client", "graph_name", "frequency", "year"],
        )

    # TODO: Documenation by Joris Marcelis
    def _make_records_of_client_targets_for_dashboard_values(self):
        df = self.transformed_data.df
        document_list = []
        for year in self.intermediate_results.List_of_years:
            document_list.append(
                dict(
                    client=self.client,
                    graph_name="has_target_client",
                    frequency="Y",
                    year=year,
                    record=extract_has_target_client(self.client, year),
                )
            )
            document_list.append(
                dict(
                    client=self.client,
                    graph_name="bis_target_client",
                    frequency="Y",
                    year=year,
                    record=extract_bis_target_client(self.client, year),
                )
            )
            document_list.append(
                dict(
                    client=self.client,
                    graph_name="werkvoorraad_bis",
                    frequency="Y",
                    year=year,
                    record=len(df[br.bis_werkvoorraad(df)]),
                )
            )
            self.records.add(
                "Overzicht_client_targets_per_jaar",
                document_list,
                DocumentListRecord,
                "Data",
                document_key=["client", "graph_name", "frequency", "year"],
            )

    def _make_records_of_voorspelling_and_planning_for_dashboard_values(self):
        """
        Calculates the overzicht values per jaar of voorspelling and planning, from which the HPend values are
        subtracted before they are shown on the dashboard. These values are extracted as a pd.Series with dates,
        based on the underlying business rules (see the functions in function_dict). The values are then calculated
        for the current year (otherwise Dashboard shows "n.v.t.") and with yearly frequency through the
        sum_over_period_to_record function. All these values are added as dictionaries to a document_list, which is
        added to the Firestore.

        Line 547 contains a temporary fix pending a new project structure. In this line, only "opgeleverdatum" values
        with ~hasdatum.isna() are returned. In a new structure, these loops can be replaced with business rule
        has_ingeplanned.

        """
        logger.info(
            "Calculating voorspelling and planning records for dashboard overview values"
        )
        # Create a dictionary that contains the output name and the appropriate mask:
        function_dict = {
            "voorspelling_minus_HPend": extract_voorspelling_dates(
                df=self.transformed_data.df,
                ftu=self.extracted_data.get("ftu"),
                totals=self.transformed_data.get("totals"),
            ),
            "planning_minus_HPend": extract_planning_dates(
                df=self.transformed_data.df,
                planning=self.transformed_data.get("planning"),
                client=self.client,
            ),
        }
        realisatie_hpend = extract_realisatie_hpend_dates(
            self.transformed_data.df[~self.transformed_data.df.hasdatum.isna()]
        )

        document_list = []
        for key, values in function_dict.items():
            record = voorspel_and_planning_minus_HPend_sum_over_periods_to_record(
                predicted=values,
                realized=realisatie_hpend,
                freq="Y",
                year=pd.Timestamp.now().strftime("%Y"),
            )
            if len(record) == 1:  # removes the date when summing over a year
                record = list(record.values())[0]
            document_list.append(
                dict(
                    client=self.client,
                    graph_name=key,  # output name from function_dict
                    frequency="Y",
                    year=pd.Timestamp.now().strftime("%Y"),
                    record=record,
                )
            )
        self.records.add(
            "Overzicht_voorspelling_planning_per_jaar",
            document_list,
            DocumentListRecord,
            "Data",
            document_key=["client", "graph_name", "frequency", "year"],
        )

    def _make_records_of_ratios_for_dashboard_values(self):
        """
        Calculates the overzicht value per jaar of ratios HC/HPend, realisatie under 8 weeks and leverbetrouwbaarheid.
        These values are extracted as pd.Series with dates, based on the underlying business rules (see the
        function_dict). The ratios are then calculated per year (obtained from _calculate_list_of_years) and with
        yearly frequency through the sum_over_period_to_record function. All these values are added as dictionaries
        to a document_list, which is added to the Firestore.

        """
        logger.info("Calculating records of ratios for dashboard overview values")
        df = self.transformed_data.df
        betrouwbaar_df = self.extracted_data.leverbetrouwbaarheid
        betrouwbaar_df["verschil_dagen"] = (
            betrouwbaar_df.hasdatum - betrouwbaar_df.last_change_in_hasdatum
        ).dt.days

        function_dict = {
            "ratio_hc_hpend": [
                extract_realisatie_hc_dates(self.transformed_data.df),
                extract_realisatie_hpend_dates(self.transformed_data.df),
            ],
            "ratio_8weeks_hpend": [
                df[
                    br.aangesloten_orders_tmobile(df=df, time_window="on time")
                ].opleverdatum,
                extract_aangesloten_orders_dates(df),
            ],
            "ratio_leverbetrouwbaarheid": [
                betrouwbaar_df[betrouwbaar_df.verschil_dagen > 3].opleverdatum,
                extract_realisatie_hpend_dates(df),
            ],
        }

        document_list = []
        for key, values in function_dict.items():
            for year in self.intermediate_results.List_of_years:
                record = ratio_sum_over_periods_to_record(
                    numerator=values[0], divider=values[1], freq="Y", year=year
                )
                if len(record) == 1:  # removes the date when summing over a year
                    record = list(record.values())[0]
                document_list.append(
                    dict(
                        client=self.client,
                        graph_name=key,  # output name from function_dict
                        frequency="Y",
                        year=year,
                        record=record,
                    )
                )
        self.records.add(
            "Overzicht_ratios_per_jaar",
            document_list,
            DocumentListRecord,
            "Data",
            document_key=["client", "graph_name", "frequency", "year"],
        )

    # TODO: remove return Tjeerd Pols
    def _make_records_ratio_hc_hpend_for_dashboard_values(self):
        """
        Calculates the overzicht value per jaar of ratio HC/HPend. This value is extracted as a pd.Series with dates,
        based on the underlying business rules (see extract_realisatie_hc_dates and extract_realisatie_hpend_dates).
        The ratio HC/HPend is then calculated per year (obtained from _calculate_list_of_years) and with yearly
        frequency through the sum_over_period_to_record function. All these values are added as dictionaries to a
        document_list, which is added to the Firestore.

        """
        logger.info(
            "Calculating record of ratio HC/HPend for dashboard overview values"
        )
        realisatie_hc = extract_realisatie_hc_dates(self.transformed_data.df)
        realisatie_hpend = extract_realisatie_hpend_dates(self.transformed_data.df)

        document_list = []
        for year in self.intermediate_results.List_of_years:
            record = ratio_sum_over_periods_to_record(
                numerator=realisatie_hc, divider=realisatie_hpend, freq="Y", year=year
            )
            # To remove the date when there is only one period (when summing over a year):
            if len(record) == 1:
                record = list(record.values())[0]
            document_list.append(
                dict(
                    client=self.client,
                    graph_name="ratio_hc_hpend",
                    frequency="Y",
                    year=year,
                    record=record,
                )
            )
        self.records.add(
            "Ratios_hc_hpend_per_jaar",
            document_list,
            DocumentListRecord,
            "Data",
            document_key=["client", "graph_name", "frequency", "year"],
        )

    # TODO: remove return Tjeerd Pols
    def _make_records_ratio_under_8weeks_for_dashboard_values(self):
        """
        Calculates the overzicht value per jaar of ratio under 8 weeks. This value is extracted as a pd.Series with
        dates, based on the underlying business rules (see br.aangesloten_orders_tmobile and
        extract_aangesloten_orders_dates). The ratio under 8 weeks is then calculated per year (obtained from
        _calculate_list_of_years) and per period ('W-MON', 'M', 'Y') through the sum_over_period_to_record function.
        All these values are added as dictionaries to a document_list, which is added to the Firestore.

        """
        logger.info(
            "Calculating record of ratio <8 weeks for dashboard overview values"
        )
        df = self.transformed_data.df
        aangesloten_orders_under_8weeks = df[
            br.aangesloten_orders_tmobile(df=df, time_window="on time")
        ].opleverdatum
        aangesloten_orders = extract_aangesloten_orders_dates(df)

        document_list = []
        for year in self.intermediate_results.List_of_years:
            record = ratio_sum_over_periods_to_record(
                numerator=aangesloten_orders_under_8weeks,
                divider=aangesloten_orders,
                freq="Y",
                year=year,
            )
            if len(record) == 1:  # removes the date when summing over a year
                record = list(record.values())[0]
            document_list.append(
                dict(
                    client=self.client,
                    graph_name="ratio_8weeks_hpend",
                    frequency="Y",
                    year=year,
                    record=record,
                )
            )
        self.records.add(
            "Ratios_under_8weeks_per_jaar",
            document_list,
            DocumentListRecord,
            "Data",
            document_key=["client", "graph_name", "frequency", "year"],
        )

    # TODO: remove return Tjeerd Pols
    def _make_intermediate_results_ratios_project_specific_values(self):
        """
        Calculates the project specific values of ratio HC/HPend, ratio under 8 weeks and HAS werkvoorraad.
        These values are extracted as a pd.Series with dates, based on the underlying business rules (see the functions
        called below). The values are then calculated per project (obtained from df.groupby('project')) with the
        calculate_ratio function OR set into a dictionary to work with calculate_projectindicators_tmobile.
        All these values are added into a dictionary with integers per project, which is added to intermediate_results.

        """
        logger.info(
            "Calculating intermediate results of ratios and HAS werkvoorraad for project specific values"
        )
        df = self.transformed_data.df
        realisatie_hc = extract_realisatie_hc_dates(df=df, add_project_column=True)
        realisatie_hpend = extract_realisatie_hpend_dates(
            df=df, add_project_column=True
        )

        aangesloten_under_8weeks = df[
            br.aangesloten_orders_tmobile(df=df, time_window="on time")
        ][["creation", "project"]]
        aangesloten_totaal = df[br.aangesloten_orders_tmobile(df=df)][
            ["creation", "project"]
        ]

        has_werkvoorraad_this_week = extract_werkvoorraad_has_dates(
            df, add_project_column=True
        )
        has_werkvoorraad_last_week = extract_werkvoorraad_has_dates(
            df, time_delta_days=7, add_project_column=True
        )

        project_dict_hc_hpend = {}
        project_dict_under_8weeks = {}
        project_dict_has_werkvoorraad = {}
        for project, df in df.groupby("project"):
            record_hc_hpend = self.calculate_ratio(
                project=project, numerator=realisatie_hc, divider=realisatie_hpend
            )
            project_dict_hc_hpend[project] = record_hc_hpend

            record_under_8weeks = self.calculate_ratio(
                project=project,
                numerator=aangesloten_under_8weeks,
                divider=aangesloten_totaal,
            )
            project_dict_under_8weeks[project] = record_under_8weeks

            # The following is to add the values for HAS werkvoorraad in the "old tmobile way" -> should be updated !!
            project_dict_has_werkvoorraad[project] = {
                "counts": len(
                    has_werkvoorraad_this_week[
                        has_werkvoorraad_this_week["project"] == project
                    ]
                ),
                "counts_prev": len(
                    has_werkvoorraad_last_week[
                        has_werkvoorraad_last_week["project"] == project
                    ]
                ),
            }

        self.intermediate_results.ratio_HC_HPend_per_project = project_dict_hc_hpend
        self.intermediate_results.ratio_under_8weeks_per_project = (
            project_dict_under_8weeks
        )
        self.intermediate_results.has_werkvoorraad_per_project = (
            project_dict_has_werkvoorraad
        )

    # TODO: remove return Tjeerd Pols
    def calculate_ratio(self, project, numerator, divider):
        """
        Calculates the ratio between the length of two pd.Series objects, filtered by a specific project.

        """
        project_dates_numerator = numerator[numerator.project == project].drop(
            labels="project", axis=1
        )
        project_dates_divider = divider[divider.project == project].drop(
            labels="project", axis=1
        )

        if len(project_dates_divider) == 0:  # to prevent zero division errors
            record = 0
        else:
            record = len(project_dates_numerator) / len(project_dates_divider)
        return record

    # TODO: Documentation by Erik van Egmond


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


class FttXETL(ETL, FttXExtract, FttXAnalyse, FttXTransform, FttXLoad):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    # TODO: Documentation by Erik van Egmond
    def perform(self):
        self.extract()
        self.transform()
        self.analyse()
        self.load()

    # TODO: Documentation by Erik van Egmond
    def document_names(self):
        return [
            value.document_name(client=self.client, graph_name=key)
            for key, value in self.records.items()
        ]

    def __repr__(self):
        return f"Analysis(client={self.client})"

    def __str__(self):
        fields = [field_name for field_name, data in self.records.items()]
        return f"Analysis(client={self.client}) containing: {fields}"

    def _repr_html_(self):
        rows = "\n".join(
            [
                data.to_table_part(field_name, self.client)
                for field_name, data in self.records.items()
            ]
        )

        table = f"""<table>
        <thead>
          <tr>
            <th>Field</th>
            <th>Collection</th>
            <th>Document</th>
          </tr>
        </thead>
        <tbody>
        {rows}
        </tbody>
        </table>"""

        return table

    # TODO: Documentation by Erik van Egmond


class FttXLocalETL(PickleExtract, FttXETL):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    # TODO: Documentation by Erik van Egmond
    def load(self):
        if "FIRESTORE_EMULATOR_HOST" in os.environ:
            logger.info("Loading into emulated firestore")
            super().load()
        else:
            logger.warning(
                "Attempting to load with a local ETL process but no emulator is configured. Loading aborted."
            )
