"""
FttX.py
============

The ETL process for FttX. It contains all steps that are common for all FttX clients.
"""

import os
import time
from datetime import timedelta
from google.cloud import firestore
from sqlalchemy import text, bindparam

from Analyse.Data import Data
from Analyse.ETL import Extract, ETL, Transform, ETLBase, Load
import pandas as pd
import numpy as np
import pickle  # nosec

import logging

from Analyse.Record.DictRecord import DictRecord
from Analyse.Record.DocumentListRecord import DocumentListRecord
from Analyse.Record.ListRecord import ListRecord
from Analyse.Record.Record import Record
import business_rules as br
from Analyse.Record.RecordListWrapper import RecordListWrapper
from functions import extract_realisatie_hpend_dates, cluster_reden_na, \
    create_project_filter, calculate_current_werkvoorraad, extract_realisatie_hc_dates, rules_to_state, \
    extract_werkvoorraad_has_dates, calculate_redenna_per_period, extract_voorspelling_dates, individual_reden_na, \
    ratio_sum_over_periods_to_record, get_database_engine, overview_reden_na, sum_over_period_to_record, \
    voorspel_and_planning_minus_HPend_sum_over_periods_to_record, extract_planning_dates, extract_target_dates, \
    extract_aangesloten_orders_dates, extract_bis_target_overview, extract_has_target_client, \
    extract_bis_target_client, get_bnumber_project_mapping

from pandas.api.types import CategoricalDtype

from toggles import ReleaseToggles

logger = logging.getLogger('FttX Analyse')

toggles = ReleaseToggles('toggles.yaml')


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
        self.project_info_location = kwargs['config'].get("project_info_location")
        self.client_name = self.config.get('name')

    # TODO: Documentation by Erik van Egmond
    def extract(self):
        """
        Extracts all data from the projects catalog for the projects set.

        Sets datasets on self.extracted_data.
        """
        logger.info("Extracting the Projects collection")
        self._extract_from_sql()
        self.extract_project_info()
        if toggles.leverbetrouwbaarheid:
            self._extract_leverbetrouwbaarheid_dataframe()

    # TODO: Documentation by Erik van Egmond
    def _extract_from_sql(self):
        logger.info("Extracting from the sql database")
        sql = text("""
select *
from fc_aansluitingen fca
where project in :projects
""").bindparams(bindparam('projects', expanding=True))  # nosec
        df = pd.read_sql(sql, get_database_engine(), params={'projects': tuple(self.projects)})
        projects_category = pd.CategoricalDtype(categories=self.projects)
        df['project'] = df.project.astype(projects_category)
        self.extracted_data.df = df

    # TODO: Documentation by Casper van Houten
    @staticmethod
    def _extract_project(project_name, cursor=None):
        start_time = time.time()
        logger.info(f"Extracting {project_name}...")
        collection = firestore.Client().collection('Projects')
        limit = 5000
        df = pd.DataFrame([])
        new_records = []
        docs = []
        while True:
            new_records.clear()
            docs.clear()
            query = collection.where('project', '==', project_name).limit(limit).order_by('__name__')
            if cursor:
                docs = [snapshot for snapshot in query.start_after(cursor).stream()]
            else:
                docs = [snapshot for snapshot in query.stream()]

            new_records = [doc.to_dict() for doc in docs]
            df = df.append(pd.DataFrame(new_records).fillna(np.nan), ignore_index=True, sort=True)

            if len(docs) == limit:
                cursor = docs[-1]
                continue
            break
        logger.info(f"Extracted {len(df)} records in {time.time() - start_time} seconds")
        return df

    def update_project_info_table(self):
        if self.project_info_location:
            bnummer_to_projectname = self.get_complete_map_bnummer_to_projectname()
            record, df_table_now = self.get_df_table_now()
            df_table_excel = self.get_df_table_excel()

            for bnummer in bnummer_to_projectname:
                # project info in table is updated with newest information from excel
                if (bnummer_to_projectname[bnummer] in df_table_now.index) & (bnummer in df_table_excel.index):
                    df_table_now.loc[bnummer_to_projectname[bnummer]] = df_table_excel.loc[bnummer]
                # table is extended with new project from excel
                elif (bnummer_to_projectname[bnummer] not in df_table_now.index) & (bnummer in df_table_excel.index):
                    df_table_now.loc[bnummer_to_projectname[bnummer]] = df_table_excel.loc[bnummer]

            df_table_now['snelheid (m/week)'] = df_table_now['meters BIS'] /\
                (df_table_now['FTU1'] - df_table_now['FTU0']).dt.days
            df_table_now.loc[~df_table_now['snelheid (m/week)'].isna(), 'snelheid (m/week)'] =\
                df_table_now[~df_table_now['snelheid (m/week)'].isna()]['snelheid (m/week)'].astype(int)

            df_table_now = df_table_now.replace({np.nan: 999, pd.NaT: 999}).replace({999: None})
            df_table_now['Civiel startdatum'] = [el.strftime('%Y-%m-%d') if el is not None else el
                                                 for el in df_table_now['Civiel startdatum']]
            df_table_now['FTU0'] = [el.strftime('%Y-%m-%d') if el is not None else el for el in df_table_now['FTU0']]
            df_table_now['FTU1'] = [el.strftime('%Y-%m-%d') if el is not None else el for el in df_table_now['FTU1']]

            record['record'] = df_table_now.to_dict(orient='dict')
            firestore.Client().collection('ProjectInfo').document('kpn_project_dates').set(record)
        else:
            raise NameError('No path to project info excel is specified')

    def get_complete_map_bnummer_to_projectname(self):
        # get mapping from table in sql database
        mapping = get_bnumber_project_mapping()
        bnummer_to_projectname_sql = pd.DataFrame(data=mapping.values(),
                                                  index=mapping.keys()).rename(columns={0: 'project'})
        bnummer_to_projectname_sql.index.name = 'bnummer'

        # get mapping from fiberconnect
        ds = self.extracted_data.df[['projectcode', 'project']].rename(columns={'projectcode': 'bnummer'})
        bnummer_to_projectname_fc = ds[~ds.duplicated()].set_index('bnummer')
        bnummer_to_projectname_fc.drop(index='8258', inplace=True)

        # get mapping from project info excel sheet
        xls = pd.ExcelFile(self.project_info_location)
        if self.client_name == 'kpn':
            test_project_info = pd.read_excel(xls, 'KPN')
            name_col_projectname = 'Milestones werkvoorbereidingsplanning KPN Projecten\n\nRevisie: E '
            name_col_projectname += '(HB-schouw voorbereiding/levering eng.)\nstatus: DEFINITIEF - d.d. '
            name_col_projectname += '18-02-2021 - Versie: 0.4'
            ds = test_project_info[['KPN B nr.', name_col_projectname]]
            ds = ds.rename(columns={'KPN B nr.': 'bnummer', name_col_projectname: 'project'})
            ds = ds[~ds['bnummer'].isna()].set_index('bnummer')
            ds.index = [str(i)[1:] if 'B' in str(i) else i for i in ds.index]
            ds.index.name = 'bnummer'
            bnummer_to_projectname_ex = ds
        else:
            raise NotImplementedError('This function needs to be extended for tmobile and dfn')

        # combine information from the three maps
        bnummer_to_projectname = bnummer_to_projectname_sql.combine_first(bnummer_to_projectname_ex).combine_first(
            bnummer_to_projectname_fc)
        bnummer_to_projectname = bnummer_to_projectname[~bnummer_to_projectname.index.isna()].to_dict()['project']

        return bnummer_to_projectname

    def get_df_table_now(self):
        record = firestore.Client().collection('ProjectInfo').document('kpn_project_dates').get().to_dict()
        df_table = pd.DataFrame.from_dict(record['record'], orient='columns').replace({'None': None})
        df_table['FTU0'] = [pd.to_datetime(el) for el in df_table['FTU0']]
        df_table['FTU1'] = [pd.to_datetime(el) for el in df_table['FTU1']]
        df_table['Civiel startdatum'] = [pd.to_datetime(el) for el in df_table['Civiel startdatum']]

        return record, df_table

    def get_df_table_excel(self):
        xls = pd.ExcelFile(self.project_info_location)
        if self.client_name == 'kpn':
            test_project_info = pd.read_excel(xls, 'KPN')
            df_table = test_project_info[['KPN B nr.',
                                          'Start Civiel',
                                          '1e BOP', 'aantal KA:',
                                          'BIS (m1)', 'doorlooptijd']]
            df_table = df_table[~df_table['KPN B nr.'].isna()].set_index('KPN B nr.')
            df_table.index = [str(i)[1:] if 'B' in str(i) else i for i in df_table.index]
            df_table.rename(columns={'Start Civiel': 'Civiel startdatum',
                                     '1e BOP': 'FTU0',
                                     'aantal KA:': 'huisaansluitingen',
                                     'BIS (m1)': 'meters BIS'},
                            inplace=True)
            df_table = df_table.replace({'???': None, '?': None}).fillna(999).replace({999: None})
            df_table['FTU0'] = [pd.to_datetime(el) for el in df_table['FTU0']]
            df_table['Civiel startdatum'] = [pd.to_datetime(el) for el in df_table['Civiel startdatum']]
            df_table['FTU1'] = [FTU0 + timedelta(w*7) if (FTU0 is not None) & (w is not None) else None
                                for FTU0, w in zip(df_table['FTU0'], df_table['doorlooptijd'])]
            df_table['snelheid (m/week)'] = [int(mBIS / w) if (mBIS is not None) & (w is not None) else None
                                             for mBIS, w in zip(df_table['meters BIS'], df_table['doorlooptijd'])]
            df_table['meters tuinschieten'] = None
            df_table = df_table.drop(columns=['doorlooptijd'])
        else:
            raise NotImplementedError('This function needs to be extended for tmobile and dfn')

        return df_table

    # TODO: Documentation by Mark Bruisten
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
        doc = firestore.Client().collection('ProjectInfo') \
            .document(f'{self.client_name}_project_dates') \
            .get().to_dict().get('record')

        self.extracted_data.ftu = Data({'date_FTU0': doc['FTU0'], 'date_FTU1': doc['FTU1']})
        self.extracted_data.civiel_startdatum = doc.get('Civiel startdatum')
        self.extracted_data.total_meters_tuinschieten = doc.get('meters tuinschieten')
        self.extracted_data.total_meters_bis = doc.get('meters BIS')
        self.extracted_data.total_number_huisaansluitingen = doc.get('huisaansluitingen')
        self.extracted_data.snelheid_mpw = doc.get('snelheid (m/week)')

        df = pd.DataFrame(doc)
        info_per_project = {}
        for project in df.index:
            info_per_project[project] = df.loc[project].to_dict()
        self.extracted_data.project_info = info_per_project

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
        sql = text("""
select  fctl.date as last_change_in_hasdatum,
        fctl.to_value as hasdatum_changed_to,
        fcas.hasdatum, fcas.opleverdatum, fcas.project
from fc_transitie_log as fctl
inner join fc_aansluitingen as fcas on
fctl.key = 'hasdatum' and
fctl.project in :projects and
fctl.sleutel = fcas.sleutel and
fctl.to_value = fcas.hasdatum and fcas.opleverdatum >= '2021-01-01'
""").bindparams(bindparam('projects', expanding=True))  # nosec
        df = pd.read_sql(sql, get_database_engine(), params={'projects': tuple(self.projects)})
        projects_category = pd.CategoricalDtype(categories=self.projects)
        df['project'] = df.project.astype(projects_category)
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
            logger.info(f"{pickle_name} not available, using fallback and pickling the result")
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

    # TODO: remove return Mark Bruisten
    def _is_ftu_available(self, project):
        """
        This functions checks whether a FTU0 date is available
        Args:
            project: the project name

        Returns:
            bool: boolean if ftu0 is available or not

        """
        available = False
        ftu0 = self.transformed_data.ftu['date_FTU0'].get(project)
        if ftu0:
            available = True
        return available

    # TODO: remove return Mark Bruisten
    def _make_project_list(self):
        """
        This functions returns a list of projects that have at least a FTU0 date.
        All the projects in this list will be evaluated in the analysis.

        Returns:
            list: returns a list of projects names
        """
        project_list = []
        if self.client == 'tmobile':
            self.project_list = self.config['projects']
        else:
            for project in self.config['projects']:
                if self._is_ftu_available(project):
                    project_list.append(project)
                else:
                    logger.warning(f'For the {project} we do not have a FTU0 date')
            self.project_list = project_list

    # TODO: Documentation by Mark Bruisten
    def _set_totals(self):
        self.transformed_data.totals = {}
        for project, project_df in self.transformed_data.df.groupby('project'):
            self.transformed_data.totals[project] = len(project_df)

    # TODO: Documentation by Erik van Egmond
    def _fix_dates(self):
        logger.info("Transforming columns to datetime column if there is 'datum' in column name")
        datums = [col for col in self.transformed_data.df.columns if
                  "datum" in col or "date" in col or "creation" in col]
        self.transformed_data.df[datums] = self.transformed_data.df[datums].apply(pd.to_datetime,
                                                                                  infer_datetime_format=True,
                                                                                  errors="coerce",
                                                                                  utc=True)

        self.transformed_data.df[datums] = self.transformed_data.df[datums].apply(lambda x: x.dt.tz_convert(None))

    def _cluster_reden_na(self):
        logger.info("Transforming dataframe through adding column cluster redenna")
        clus = self.config['clusters_reden_na']
        self.transformed_data.df.loc[:, 'cluster_redenna'] = self.transformed_data.df['redenna'].apply(
            lambda x: cluster_reden_na(x, clus))
        self.transformed_data.df.loc[br.hc_opgeleverd(self.transformed_data.df), ['cluster_redenna']] = 'HC'
        cluster_types = CategoricalDtype(categories=list(clus.keys()), ordered=True)
        self.transformed_data.df['cluster_redenna'] = self.transformed_data.df['cluster_redenna'].astype(cluster_types)

    # TODO: Documentation by Erik van Egmond
    def _add_status_columns(self):
        logger.info("Transforming dataframe through adding status columns")
        state_list = ['niet_opgeleverd', "ingeplanned", "opgeleverd_zonder_hc", "opgeleverd"]
        self.transformed_data.df['false'] = False
        has_rules_list = [
            br.has_niet_opgeleverd(self.transformed_data.df),
            br.has_ingeplanned(self.transformed_data.df),
            br.hp_opgeleverd(self.transformed_data.df),
            br.hc_opgeleverd(self.transformed_data.df)
        ]
        logger.info("Added has_rules_list")
        has = rules_to_state(has_rules_list, state_list)
        geschouwd_rules_list = [
            ~ br.toestemming_bekend(self.transformed_data.df),
            self.transformed_data.df['false'],
            self.transformed_data.df['false'],
            br.toestemming_bekend(self.transformed_data.df)
        ]
        geschouwd = rules_to_state(geschouwd_rules_list, state_list)
        logger.info("Added geschouwd_rules_list")

        bis_gereed_rules_list = [
            br.bis_niet_opgeleverd(self.transformed_data.df),
            self.transformed_data.df['false'],
            self.transformed_data.df['false'],
            br.bis_opgeleverd(self.transformed_data.df)
        ]
        bis_gereed = rules_to_state(bis_gereed_rules_list, state_list)
        logger.info("Added bis_gereed_rules_list")

        laswerkdpgereed_rules_list = [
            br.laswerk_dp_niet_gereed(self.transformed_data.df),
            self.transformed_data.df['false'],
            self.transformed_data.df['false'],
            br.laswerk_dp_gereed(self.transformed_data.df)
        ]
        laswerkdpgereed = rules_to_state(laswerkdpgereed_rules_list, state_list)

        logger.info("Added laswerkdpgereed_rules_list")

        laswerkapgereed_rules_list = [
            br.laswerk_ap_niet_gereed(self.transformed_data.df),
            self.transformed_data.df['false'],
            self.transformed_data.df['false'],
            br.laswerk_ap_gereed(self.transformed_data.df)
        ]
        laswerkapgereed = rules_to_state(laswerkapgereed_rules_list, state_list)

        logger.info("Added laswerkapgereed_rules_list")
        business_rules_list = [
            [geschouwd, "schouw_status"],
            [bis_gereed, "bis_status"],
            [self.transformed_data.df['soort_bouw'] == 'Laag', "laagbouw"],
            [laswerkdpgereed, "lasDP_status"],
            [laswerkapgereed, "lasAP_status"],
            [has, "HAS_status"],

        ]
        neccesary_info_list = [
            [self.transformed_data.df['sleutel'], "sleutel"],
        ]

        series_list = business_rules_list + neccesary_info_list

        cols, colnames = list(zip(*series_list))
        status_df = pd.concat(cols, axis=1)
        status_df.columns = colnames
        self.transformed_data.df.drop('false', inplace=True, axis=1)
        self.transformed_data.df = pd.merge(self.transformed_data.df, status_df, on="sleutel", how="left")


# TODO: Documentation by Erik van Egmond
class FttXAnalyse(FttXBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not hasattr(self, 'config'):
            self.config = kwargs.get("config")
        self.records = RecordListWrapper(self.client)
        self.intermediate_results = Data()

    # TODO: Documentation by Andre van Turnhout
    def analyse(self):
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
            columns = ['opleverdatum', 'schouwdatum', 'laswerkapgereed_datum', 'laswerkdpgereed_datum',
                       'status_civiel_datum', 'laswerkapgereed', 'laswerkdpgereed']
            date_df = df.loc[:, columns]
            mask = br.laswerk_dp_gereed(df) & br.laswerk_ap_gereed(df)
            date_df['montage'] = np.datetime64("NaT")
            date_df.loc[mask, 'montage'] = date_df[['laswerkapgereed_datum', 'laswerkdpgereed_datum']][mask].max(axis=1)
            date_df = date_df.drop(columns=['laswerkapgereed', 'laswerkdpgereed'])
            progress_over_time: pd.DataFrame = date_df.apply(pd.value_counts).resample("D").sum().cumsum() / len(
                df)
            progress_over_time.index = progress_over_time.index.strftime("%Y-%m-%d")
            progress_over_time.rename(columns={'opleverdatum': 'has',
                                               'schouwdatum': 'schouwen',
                                               'laswerkapgereed_datum': 'montage ap',
                                               'laswerkdpgereed_datum': 'montage dp',
                                               'status_civiel_datum': 'civiel',
                                               },
                                      inplace=True
                                      )
            record = progress_over_time.to_dict()
            document_list.append(dict(
                client=self.client,
                project=project,
                data_set="progress_over_time",
                record=record
            ))
        self.records.add("Progress_over_time", document_list, DocumentListRecord, "Data",
                         document_key=["client", "project", 'data_set'])

    # TODO: Documentation by Andre van Turnhout
    def _progress_per_phase(self):
        logger.info("Calculating project progress per phase")

        progress_df = pd.concat(
            [
                self.transformed_data.df.project,
                ~self.transformed_data.df.sleutel.isna(),
                self.transformed_data.df.status_civiel == '1',
                br.laswerk_dp_gereed(self.transformed_data.df) & br.laswerk_ap_gereed(self.transformed_data.df),
                br.geschouwed(self.transformed_data.df),
                br.hc_opgeleverd(self.transformed_data.df),
                br.hp_opgeleverd(self.transformed_data.df),
                br.opgeleverd(self.transformed_data.df)
            ],
            axis=1
        )
        progress_df.columns = [
            'project',
            'totaal',
            'civiel',
            'montage',
            'schouwen',
            'hc',
            'hp',
            'hpend'
        ]
        documents = [dict(project=project, client=self.client, data_set="progress", record=values) for project, values
                     in
                     progress_df.groupby('project').sum().to_dict(orient="index").items()]

        self.records.add("Progress", documents, DocumentListRecord, "Data",
                         document_key=["client", "project", 'data_set'])

    # TODO: remove return Tjeerd Pols
    def _calculate_list_of_years(self):
        """
        Calculates a list of years per client based on the dates that are found in the date columns.

        Returns:
            list: a list of years (as individual strings), sorted per year

        """
        logger.info("Calculating list of years")
        date_columns = [col for col in self.transformed_data.df.columns if
                        "datum" in col or "date" in col or "creation" in col]
        dc_data = self.transformed_data.df.loc[:, date_columns]
        list_of_years = []
        for col in dc_data.columns:
            list_of_years += list(dc_data[col].dropna().dt.year.unique().astype(str))
        list_of_years = sorted(list(set(list_of_years)))

        self.records.add('List_of_years', list_of_years, Record, 'Data')
        self.intermediate_results.List_of_years = list_of_years

    # TODO: Documentation by Erik van Egmond
    def _calculate_current_werkvoorraad(self):
        logger.info("Calculating y voorraad act for KPN")
        current_werkvoorraad = calculate_current_werkvoorraad(self.transformed_data.df)
        self.intermediate_results.current_werkvoorraad = current_werkvoorraad

    # TODO: Documentation by Casper van Houten
    def _reden_na(self):
        logger.info("Calculating reden na graphs")
        overview_record = overview_reden_na(self.transformed_data.df)
        record_dict = individual_reden_na(self.transformed_data.df)
        self.records.add('reden_na_overview', overview_record, Record, 'Data')
        self.records.add('reden_na_projects', record_dict, DictRecord, 'Data')

    # TODO: Documentation by Casper van Houten
    def _set_filters(self):
        self.records.add("project_names", create_project_filter(self.transformed_data.df), ListRecord, "Data")

    # TODO: Documentation by Erik van Egmond
    def _calculate_status_counts_per_project(self):
        logger.info("Calculating completed status counts per project")

        status_df: pd.DataFrame = self.transformed_data.df[['schouw_status', 'bis_status', 'laagbouw', 'lasDP_status',
                                                            'lasAP_status', 'HAS_status', 'cluster_redenna', 'sleutel',
                                                            'project']]
        status_df = status_df.rename(columns={"sleutel": "count"})
        status_counts_dict = {}
        col_names = list(status_df.columns)
        for project in status_df.project.unique():
            project_status = status_df[status_df.project == project][col_names[:-1]]
            status_counts_dict[project] = project_status.groupby(col_names[:-2]) \
                .count() \
                .reset_index() \
                .dropna() \
                .to_dict(orient='records')
        self.records.add('completed_status_counts', status_counts_dict, DictRecord, 'Data')

    # TODO: Documentation by Casper van Houten
    def _calculate_redenna_per_period(self):
        logger.info("Calculating redenna per period (week & month)")
        by_week = calculate_redenna_per_period(df=self.transformed_data.df,
                                               date_column="hasdatum",
                                               freq="W-MON")
        self.records.add('redenna_by_week', by_week, Record, 'Data')

        by_month = calculate_redenna_per_period(df=self.transformed_data.df,
                                                date_column="hasdatum",
                                                freq="M")
        self.records.add('redenna_by_month', by_month, Record, 'Data')

        by_year = calculate_redenna_per_period(df=self.transformed_data.df,
                                               date_column="hasdatum",
                                               freq="Y")
        self.records.add('redenna_by_year', by_year, Record, 'Data')

    # TODO: remove return Tjeerd Pols
    def _make_records_for_dashboard_values(self):
        """
        Calculates the overzicht values per jaar of simple KPI's that do not contain a ratio or need a subtraction.
        These values are extracted as a pd.Series with dates, based on the underlying business rules (see the functions
        in function_dict). The values are then calculated per year (obtained from _calculate_list_of_years) and per
        period ('W-MON', 'M', 'Y') through the sum_over_period_to_record function. All these values are added as
        dictionaries to a document_list, which is added to the Firestore.

        Returns:
             list: a list with dictionaries containing the relevant values

        """
        logger.info("Calculating records for dashboard overview values")
        df = self.transformed_data.df
        # Create a dictionary that contains the output name and the appropriate mask:
        function_dict = {'realisatie_bis': df[br.bis_opgeleverd(df)].status_civiel_datum,
                         'werkvoorraad_has': extract_werkvoorraad_has_dates(df),
                         'realisatie_hpend': extract_realisatie_hpend_dates(df),
                         'target_intern_bis': extract_bis_target_overview(
                             civiel_startdatum=self.transformed_data.get('civiel_startdatum'),
                             total_meters_bis=self.transformed_data.get('total_meters_bis'),
                             total_num_has=self.transformed_data.get('total_number_huisaansluitingen'),
                             snelheid_m_week=self.transformed_data.get('snelheid_mpw'),
                             client=self.client),  # TODO: Remove when project info is available for tmobile and dfn
                         'target': extract_target_dates(df=df,
                                                        project_list=self.project_list,
                                                        totals=self.transformed_data.get("totals"),
                                                        ftu=self.extracted_data.get("ftu")),
                         'voorspelling': extract_voorspelling_dates(df=df,
                                                                    ftu=self.extracted_data.get("ftu"),
                                                                    totals=self.transformed_data.get("totals")),
                         'planning': extract_planning_dates(df=df,
                                                            planning=self.transformed_data.get("planning"),
                                                            client=self.client)
                         }
        list_of_freq = ['W-MON', 'M', 'Y']

        document_list = []
        for key, values in function_dict.items():
            for year in self.intermediate_results.List_of_years:
                for freq in list_of_freq:
                    record = sum_over_period_to_record(timeseries=values, freq=freq, year=year)
                    if len(record) == 1:  # removes the date when summing over a year
                        record = list(record.values())[0]
                    document_list.append(dict(client=self.client,
                                              graph_name=key,  # output name from function_dict
                                              frequency=freq,
                                              year=year,
                                              record=record))
        self.records.add("Overzicht_per_jaar", document_list, DocumentListRecord, "Data",
                         document_key=["client", "graph_name", "frequency", "year"])

    # TODO: Documenation by Joris Marcelis
    def _make_records_of_client_targets_for_dashboard_values(self):
        df = self.transformed_data.df
        document_list = []
        for year in self.intermediate_results.List_of_years:
            document_list.append(dict(client=self.client,
                                      graph_name='has_target_client',
                                      frequency='Y',
                                      year=year,
                                      record=extract_has_target_client(self.client, year)))
            document_list.append(dict(client=self.client,
                                      graph_name='bis_target_client',
                                      frequency='Y',
                                      year=year,
                                      record=extract_bis_target_client(self.client, year)))
            document_list.append(dict(client=self.client,
                                      graph_name='werkvoorraad_bis',
                                      frequency='Y',
                                      year=year,
                                      record=len(df[br.bis_werkvoorraad(df)])))
            self.records.add("Overzicht_client_targets_per_jaar", document_list, DocumentListRecord, "Data",
                             document_key=["client", "graph_name", "frequency", "year"])

    # TODO: remove return Tjeerd Pols
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

        Returns: a list with dictionaries containing the values for voorspelling and planning

        """
        logger.info("Calculating voorspelling and planning records for dashboard overview values")
        # Create a dictionary that contains the output name and the appropriate mask:
        function_dict = {'voorspelling_minus_HPend': extract_voorspelling_dates(
            df=self.transformed_data.df,
            ftu=self.extracted_data.get("ftu"),
            totals=self.transformed_data.get("totals")),
            'planning_minus_HPend': extract_planning_dates(df=self.transformed_data.df,
                                                           planning=self.transformed_data.get("planning"),
                                                           client=self.client),
        }
        realisatie_hpend = extract_realisatie_hpend_dates(
            self.transformed_data.df[~self.transformed_data.df.hasdatum.isna()])

        document_list = []
        for key, values in function_dict.items():
            record = voorspel_and_planning_minus_HPend_sum_over_periods_to_record(predicted=values,
                                                                                  realized=realisatie_hpend,
                                                                                  freq='Y',
                                                                                  year=pd.Timestamp.now().strftime(
                                                                                      "%Y"))
            if len(record) == 1:  # removes the date when summing over a year
                record = list(record.values())[0]
            document_list.append(dict(client=self.client,
                                      graph_name=key,  # output name from function_dict
                                      frequency='Y',
                                      year=pd.Timestamp.now().strftime("%Y"),
                                      record=record))
        self.records.add("Overzicht_voorspelling_planning_per_jaar", document_list, DocumentListRecord, "Data",
                         document_key=["client", "graph_name", "frequency", "year"])

    def _make_records_of_ratios_for_dashboard_values(self):
        """
        Calculates the overzicht value per jaar of ratios HC/HPend, realisatie under 8 weeks and leverbetrouwbaarheid.
        These values are extracted as pd.Series with dates, based on the underlying business rules (see the
        function_dict). The ratios are then calculated per year (obtained from _calculate_list_of_years) and with
        yearly frequency through the sum_over_period_to_record function. All these values are added as dictionaries
        to a document_list, which is added to the Firestore.

        Returns: a list with dicts containing the ratios HC/HPend, realisatie under 8 weeks and leverbetrouwbaarheid.

        """
        logger.info("Calculating records of ratios for dashboard overview values")
        df = self.transformed_data.df
        betrouwbaar_df = self.extracted_data.leverbetrouwbaarheid
        betrouwbaar_df['verschil_dagen'] = (betrouwbaar_df.hasdatum - betrouwbaar_df.last_change_in_hasdatum).dt.days

        function_dict = {
            "ratio_hc_hpend": [extract_realisatie_hc_dates(self.transformed_data.df),
                               extract_realisatie_hpend_dates(self.transformed_data.df)],
            "ratio_8weeks_hpend": [df[br.aangesloten_orders_tmobile(df=df, time_window="on time")].opleverdatum,
                                   extract_aangesloten_orders_dates(df)],
            "ratio_leverbetrouwbaarheid": [betrouwbaar_df[betrouwbaar_df.verschil_dagen > 3].opleverdatum,
                                           extract_realisatie_hpend_dates(df)]
        }

        document_list = []
        for key, values in function_dict.items():
            for year in self.intermediate_results.List_of_years:
                record = ratio_sum_over_periods_to_record(numerator=values[0], divider=values[1],
                                                          freq='Y', year=year)
                if len(record) == 1:  # removes the date when summing over a year
                    record = list(record.values())[0]
                document_list.append(dict(client=self.client,
                                          graph_name=key,  # output name from function_dict
                                          frequency='Y',
                                          year=year,
                                          record=record))
        self.records.add("Overzicht_ratios_per_jaar", document_list, DocumentListRecord, "Data",
                         document_key=["client", "graph_name", "frequency", "year"])

    # TODO: remove return Tjeerd Pols
    def _make_records_ratio_hc_hpend_for_dashboard_values(self):
        """
        Calculates the overzicht value per jaar of ratio HC/HPend. This value is extracted as a pd.Series with dates,
        based on the underlying business rules (see extract_realisatie_hc_dates and extract_realisatie_hpend_dates).
        The ratio HC/HPend is then calculated per year (obtained from _calculate_list_of_years) and with yearly
        frequency through the sum_over_period_to_record function. All these values are added as dictionaries to a
        document_list, which is added to the Firestore.

        Returns: a list with dictionaries containing the ratio HC/HPend

        """
        logger.info("Calculating record of ratio HC/HPend for dashboard overview values")
        realisatie_hc = extract_realisatie_hc_dates(self.transformed_data.df)
        realisatie_hpend = extract_realisatie_hpend_dates(self.transformed_data.df)

        document_list = []
        for year in self.intermediate_results.List_of_years:
            record = ratio_sum_over_periods_to_record(numerator=realisatie_hc, divider=realisatie_hpend,
                                                      freq='Y', year=year)
            # To remove the date when there is only one period (when summing over a year):
            if len(record) == 1:
                record = list(record.values())[0]
            document_list.append(dict(client=self.client,
                                      graph_name='ratio_hc_hpend',
                                      frequency='Y',
                                      year=year,
                                      record=record))
        self.records.add("Ratios_hc_hpend_per_jaar", document_list, DocumentListRecord, "Data",
                         document_key=["client", "graph_name", "frequency", "year"])

    # TODO: remove return Tjeerd Pols
    def _make_records_ratio_under_8weeks_for_dashboard_values(self):
        """
        Calculates the overzicht value per jaar of ratio under 8 weeks. This value is extracted as a pd.Series with
        dates, based on the underlying business rules (see br.aangesloten_orders_tmobile and
        extract_aangesloten_orders_dates). The ratio under 8 weeks is then calculated per year (obtained from
        _calculate_list_of_years) and per period ('W-MON', 'M', 'Y') through the sum_over_period_to_record function.
        All these values are added as dictionaries to a document_list, which is added to the Firestore.

        Returns: a list with dictionaries containing the ratio under 8 weeks

        """
        logger.info("Calculating record of ratio <8 weeks for dashboard overview values")
        df = self.transformed_data.df
        aangesloten_orders_under_8weeks = df[br.aangesloten_orders_tmobile(df=df,
                                                                           time_window="on time")].opleverdatum
        aangesloten_orders = extract_aangesloten_orders_dates(df)

        document_list = []
        for year in self.intermediate_results.List_of_years:
            record = ratio_sum_over_periods_to_record(numerator=aangesloten_orders_under_8weeks,
                                                      divider=aangesloten_orders,
                                                      freq='Y', year=year)
            if len(record) == 1:  # removes the date when summing over a year
                record = list(record.values())[0]
            document_list.append(dict(client=self.client,
                                      graph_name='ratio_8weeks_hpend',
                                      frequency='Y',
                                      year=year,
                                      record=record))
        self.records.add("Ratios_under_8weeks_per_jaar", document_list, DocumentListRecord, "Data",
                         document_key=["client", "graph_name", "frequency", "year"])

    # TODO: remove return Tjeerd Pols
    def _make_intermediate_results_ratios_project_specific_values(self):
        """
        Calculates the project specific values of ratio HC/HPend, ratio under 8 weeks and HAS werkvoorraad.
        These values are extracted as a pd.Series with dates, based on the underlying business rules (see the functions
        called below). The values are then calculated per project (obtained from df.groupby('project')) with the
        calculate_ratio function OR set into a dictionary to work with calculate_projectindicators_tmobile.
        All these values are added into a dictionary with integers per project, which is added to intermediate_results.

        Returns: dictionaries with the values per project

        """
        logger.info("Calculating intermediate results of ratios and HAS werkvoorraad for project specific values")
        df = self.transformed_data.df
        realisatie_hc = extract_realisatie_hc_dates(df=df, add_project_column=True)
        realisatie_hpend = extract_realisatie_hpend_dates(df=df, add_project_column=True)

        aangesloten_under_8weeks = df[br.aangesloten_orders_tmobile(df=df, time_window='on time')][['creation',
                                                                                                    'project']]
        aangesloten_totaal = df[br.aangesloten_orders_tmobile(df=df)][['creation', 'project']]

        has_werkvoorraad_this_week = extract_werkvoorraad_has_dates(df, add_project_column=True)
        has_werkvoorraad_last_week = extract_werkvoorraad_has_dates(df, time_delta_days=7, add_project_column=True)

        project_dict_hc_hpend = {}
        project_dict_under_8weeks = {}
        project_dict_has_werkvoorraad = {}
        for project, df in df.groupby('project'):
            record_hc_hpend = self.calculate_ratio(project=project,
                                                   numerator=realisatie_hc,
                                                   divider=realisatie_hpend)
            project_dict_hc_hpend[project] = record_hc_hpend

            record_under_8weeks = self.calculate_ratio(project=project,
                                                       numerator=aangesloten_under_8weeks,
                                                       divider=aangesloten_totaal)
            project_dict_under_8weeks[project] = record_under_8weeks

            # The following is to add the values for HAS werkvoorraad in the "old tmobile way" -> should be updated !!
            project_dict_has_werkvoorraad[project] = {
                'counts': len(has_werkvoorraad_this_week[has_werkvoorraad_this_week['project'] == project]),
                'counts_prev': len(has_werkvoorraad_last_week[has_werkvoorraad_last_week['project'] == project])}

        self.intermediate_results.ratio_HC_HPend_per_project = project_dict_hc_hpend
        self.intermediate_results.ratio_under_8weeks_per_project = project_dict_under_8weeks
        self.intermediate_results.has_werkvoorraad_per_project = project_dict_has_werkvoorraad

    # TODO: remove return Tjeerd Pols
    def calculate_ratio(self, project, numerator, divider):
        """
        Calculates the ratio between the length of two pd.Series objects, filtered by a specific project.

        Returns: a ratio between two pd.Series as an integer

        """
        project_dates_numerator = numerator[numerator.project == project].drop(labels='project', axis=1)
        project_dates_divider = divider[divider.project == project].drop(labels='project', axis=1)

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
        return [value.document_name(client=self.client, graph_name=key) for key, value in self.records.items()]

    def __repr__(self):
        return f"Analysis(client={self.client})"

    def __str__(self):
        fields = [field_name for field_name, data in self.records.items()]
        return f"Analysis(client={self.client}) containing: {fields}"

    def _repr_html_(self):
        rows = "\n".join(
            [data.to_table_part(field_name, self.client)
             for field_name, data in self.records.items()
             ])

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
        if 'FIRESTORE_EMULATOR_HOST' in os.environ:
            logger.info("Loading into emulated firestore")
            super().load()
        else:
            logger.warning(
                "Attempting to load with a local ETL process but no emulator is configured. Loading aborted.")
