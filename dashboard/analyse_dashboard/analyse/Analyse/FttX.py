import os
import time
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
    create_project_filter, calculate_y_voorraad_act, extract_realisatie_hc_dates, rules_to_state, \
    extract_werkvoorraad_has_dates, calculate_redenna_per_period, \
    calculate_projectspecs, extract_voorspelling_dates, individual_reden_na, \
    ratio_sum_over_periods_to_record, get_database_engine, \
    overview_reden_na, sum_over_period_to_record, voorspel_and_planning_minus_HPend_sum_over_periods_to_record, \
    extract_planning_dates, extract_target_dates, extract_realisatie_hpend_and_ordered_dates
from pandas.api.types import CategoricalDtype

from toggles import ReleaseToggles

logger = logging.getLogger('FttX Analyse')

toggles = ReleaseToggles('toggles.yaml')


class FttXBase(ETLBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not hasattr(self, 'config'):
            self.config = kwargs.get("config")
        self.client = kwargs.get("client", "client_unknown")
        self.record_dict = RecordListWrapper(client=self.client)
        self.intermediate_results = Data()


class FttXExtract(Extract):

    def __init__(self, **kwargs):
        if not hasattr(self, 'config'):
            self.config = kwargs.get("config")
        if not self.config:
            raise ValueError("No config provided in init")
        self.projects = self.config["projects"]
        self.client_name = kwargs['config'].get('name')
        super().__init__(**kwargs)

    def extract(self):
        """Extracts all data from the projects catalog for the projects set.
        Sets self.extracted_data to a pd.Dataframe of all data.
        """
        logger.info("Extracting the Projects collection")
        if toggles.fc_sql:
            self._extract_from_sql()
        else:
            self._extract_from_firestore()
        self._extract_project_info()
        self._extract_planning()

    def _extract_from_firestore(self):
        logger.info("Extracting from the firestore")
        df = pd.DataFrame([])
        for key in self.projects:
            df = df.append(self._extract_project(key), ignore_index=True, sort=True)
        projects_category = pd.CategoricalDtype(categories=self.projects)
        df['project'] = df.project.astype(projects_category)
        self.extracted_data.df = df

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

    def _extract_project_info(self):
        logger.info(f"Extracting FTU {self.client_name}")
        doc = firestore.Client().collection('Data') \
            .document(f'{self.client_name}_project_dates') \
            .get().to_dict().get('record')

        self.extracted_data.ftu = Data({'date_FTU0': doc['FTU0'], 'date_FTU1': doc['FTU1']})
        self.extracted_data.civiel_startdatum = doc.get('Civiel startdatum')
        self.extracted_data.total_meters_tuinschieten = doc.get('meters tuinschieten')
        self.extracted_data.total_meters_bis = doc.get('meters BIS')
        self.extracted_data.total_number_huisaansluitingen = doc.get('huisaansluitingen')

        df = pd.DataFrame(doc)
        info_per_project = {}
        for project in df.index:
            info_per_project[project] = df.loc[project].to_dict()
        self.extracted_data.project_info = info_per_project

    def _extract_planning(self):
        logger.info("Extracting Planning")
        if hasattr(self, 'planning_location'):
            if 'gs://' in self.planning_location:
                xls = pd.ExcelFile(self.planning_location)
            else:
                xls = pd.ExcelFile(self.planning_location)
            df = pd.read_excel(xls, 'FTTX ').fillna(0)
            self.extracted_data.planning = df
        else:
            self.extracted_data.planning = pd.DataFrame()


class PickleExtract(Extract, FttXBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

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


class FttXTransform(Transform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.year = kwargs.get("year", str(pd.Timestamp.now().year))

    def transform(self, **kwargs):
        super().transform()
        logger.info("Transforming the data following the FttX protocol")
        self._fix_dates()
        self._transform_planning()
        self._add_columns()
        self._cluster_reden_na()
        self._add_status_columns()
        self._set_totals()
        self._clean_ftu_data()
        self._make_project_list()

    def _clean_ftu_data(self):
        for key in ['date_FTU0', 'date_FTU1']:
            for project, date in self.extracted_data.ftu[key].items():
                if date == '' or date == 'None':
                    date = None
                self.transformed_data.ftu[key][project] = date

    def _is_ftu_available(self, project):
        available = False
        ftu0 = self.transformed_data.ftu['date_FTU0'].get(project)
        ftu1 = self.transformed_data.ftu['date_FTU1'].get(project)
        if ftu0 and ftu1:
            available = True
        return available

    def _is_data_available(self, project):
        available = False
        if project in self.transformed_data.df.project.unique():
            available = True
        return available

    def _make_project_list(self):
        project_list = []
        for project in self.config['projects']:
            ftu_avaiable = self._is_ftu_available(project)
            data_available = self._is_data_available(project)
            if ftu_avaiable and data_available:
                project_list.append(project)
        self.project_list = project_list

    def _set_totals(self):
        self.transformed_data.totals = {}
        for project, project_df in self.transformed_data.df.groupby('project'):
            self.transformed_data.totals[project] = len(project_df)

    def _fix_dates(self):
        logger.info("Changing columns to datetime column if there is 'datum' in column name.")
        datums = [col for col in self.transformed_data.df.columns if "datum" in col or "date" in col or "creation" in col]
        self.transformed_data.df[datums] = self.transformed_data.df[datums].apply(pd.to_datetime,
                                                                                  infer_datetime_format=True,
                                                                                  errors="coerce",
                                                                                  utc=True)

        self.transformed_data.df[datums] = self.transformed_data.df[datums].apply(lambda x: x.dt.tz_convert(None))

    def _add_columns(self):
        logger.info("Adding columns to dataframe")

        self.transformed_data.df['hpend'] = br.hpend_year(self.transformed_data.df, self.year)
        self.transformed_data.df['homes_completed'] = br.hc_opgeleverd(self.transformed_data.df) & (
            self.transformed_data.df.hpend)
        self.transformed_data.df['homes_completed_total'] = br.hc_opgeleverd(self.transformed_data.df)
        self.transformed_data.df['bis_gereed'] = br.bis_opgeleverd(self.transformed_data.df)
        self.transformed_data.df['in_has_werkvoorraad'] = br.has_werkvoorraad(self.transformed_data.df)

    def _cluster_reden_na(self):
        logger.info("Adding column cluster redenna to dataframe")
        clus = self.config['clusters_reden_na']
        self.transformed_data.df.loc[:, 'cluster_redenna'] = self.transformed_data.df['redenna'].apply(
            lambda x: cluster_reden_na(x, clus))
        self.transformed_data.df.loc[br.hc_opgeleverd(self.transformed_data.df), ['cluster_redenna']] = 'HC'
        cluster_types = CategoricalDtype(categories=list(clus.keys()), ordered=True)
        self.transformed_data.df['cluster_redenna'] = self.transformed_data.df['cluster_redenna'].astype(cluster_types)

    def _add_status_columns(self):
        logger.info("Adding status columns to dataframe")
        state_list = ['niet_opgeleverd', "ingeplanned", "opgeleverd_zonder_hc", "opgeleverd"]
        self.transformed_data.df['false'] = False
        has_rules_list = [
            br.has_niet_opgeleverd(self.transformed_data.df),
            br.has_ingeplanned(self.transformed_data.df),
            br.hp_opgeleverd(self.transformed_data.df),
            br.hc_opgeleverd(self.transformed_data.df)
        ]
        has = rules_to_state(has_rules_list, state_list)
        geschouwd_rules_list = [
            ~ br.toestemming_bekend(self.transformed_data.df),
            self.transformed_data.df['false'],
            self.transformed_data.df['false'],
            br.toestemming_bekend(self.transformed_data.df)
        ]
        geschouwd = rules_to_state(geschouwd_rules_list, state_list)

        bis_gereed_rules_list = [
            br.bis_niet_opgeleverd(self.transformed_data.df),
            self.transformed_data.df['false'],
            self.transformed_data.df['false'],
            br.bis_opgeleverd(self.transformed_data.df)
        ]
        bis_gereed = rules_to_state(bis_gereed_rules_list, state_list)

        laswerkdpgereed_rules_list = [
            br.laswerk_dp_niet_gereed(self.transformed_data.df),
            self.transformed_data.df['false'],
            self.transformed_data.df['false'],
            br.laswerk_dp_gereed(self.transformed_data.df)
        ]
        laswerkdpgereed = rules_to_state(laswerkdpgereed_rules_list, state_list)

        laswerkapgereed_rules_list = [
            br.laswerk_ap_niet_gereed(self.transformed_data.df),
            self.transformed_data.df['false'],
            self.transformed_data.df['false'],
            br.laswerk_ap_gereed(self.transformed_data.df)
        ]
        laswerkapgereed = rules_to_state(laswerkapgereed_rules_list, state_list)

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

    def _transform_planning(self):
        logger.info("Transforming planning for KPN")
        HP = dict(HPendT=[0] * 52)
        df = self.extracted_data.get("planning", pd.DataFrame())
        if not df.empty:
            for el in df.index:  # Arnhem Presikhaaf toevoegen aan subset??
                if df.loc[el, ('Unnamed: 1')] == 'HP+ Plan':
                    HP[df.loc[el, ('Unnamed: 0')]] = df.loc[el][16:68].to_list()
                    HP['HPendT'] = [sum(x) for x in zip(HP['HPendT'], HP[df.loc[el, ('Unnamed: 0')]])]
                    if df.loc[el, ('Unnamed: 0')] == 'Bergen op Zoom Oude Stad':
                        HP['Bergen op Zoom oude stad'] = HP.pop(df.loc[el, ('Unnamed: 0')])
                    if df.loc[el, ('Unnamed: 0')] == 'Arnhem Gulden Bodem':
                        HP['Arnhem Gulden Bodem Schaarsbergen'] = HP.pop(df.loc[el, ('Unnamed: 0')])
                    if df.loc[el, ('Unnamed: 0')] == 'Bergen op Zoom Noord':
                        HP['Bergen op Zoom Noord\xa0 wijk 01\xa0+ Halsteren'] = HP.pop(df.loc[el, ('Unnamed: 0')])
                    if df.loc[el, ('Unnamed: 0')] == 'Den Haag Bezuidenhout':
                        HP['Den Haag - Haagse Hout-Bezuidenhout West'] = HP.pop(df.loc[el, ('Unnamed: 0')])
                    if df.loc[el, ('Unnamed: 0')] == 'Den Haag Morgenstond':
                        HP['Den Haag Morgenstond west'] = HP.pop(df.loc[el, ('Unnamed: 0')])
                    if df.loc[el, ('Unnamed: 0')] == 'Den Haag Vrederust Bouwlust':
                        HP['Den Haag - Vrederust en Bouwlust'] = HP.pop(df.loc[el, ('Unnamed: 0')])
                    if df.loc[el, ('Unnamed: 0')] == '':
                        HP['KPN Spijkernisse'] = HP.pop(df.loc[el, ('Unnamed: 0')])
                    if df.loc[el, ('Unnamed: 0')] == 'Gouda Kort Haarlem':
                        HP['KPN Gouda Kort Haarlem en Noord'] = HP.pop(df.loc[el, ('Unnamed: 0')])
        else:
            HP = {}
        self.transformed_data.planning = HP


class FttXAnalyse(FttXBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not hasattr(self, 'config'):
            self.config = kwargs.get("config")
        self.record_dict = RecordListWrapper(self.client)
        self.intermediate_results = Data()

    def analyse(self):
        logger.info("Analysing using the FttX protocol")
        self._calculate_list_of_years()
        self._make_records_for_dashboard_values(self.project_list)
        self._make_records_of_voorspelling_and_planning_for_dashboard_values()
        self._make_records_ratio_hc_hpend_for_dashboard_values()
        self._make_records_ratio_under_8weeks_for_dashboard_values()
        if toggles.new_projectspecific_views:
            self._make_intermediate_results_ratios_project_specific_values()
        else:
            self._calculate_projectspecs()
        self._calculate_y_voorraad_act()
        self._reden_na()
        self._set_filters()
        self._calculate_status_counts_per_project()
        self._calculate_redenna_per_period()
        self._progress_per_phase()
        self._progress_per_phase_over_time()

    def _progress_per_phase_over_time(self):
        logger.info("Calculating project progress per phase over time")
        document_list = []
        for project, df in self.transformed_data.df.groupby("project"):
            if project in self.project_list:
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
            self.record_dict.add("Progress_over_time", document_list, DocumentListRecord, "Data",
                                 document_key=["client", "project", 'data_set'])

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

        self.record_dict.add("Progress", documents, DocumentListRecord, "Data",
                             document_key=["client", "project", 'data_set'])

    def _calculate_list_of_years(self):
        logger.info("Calculating list of years")
        date_columns = [col for col in self.transformed_data.df.columns if "datum" in col or "date" in col or "creation" in col]
        dc_data = self.transformed_data.df.loc[:, date_columns]
        list_of_years = []
        for col in dc_data.columns:
            list_of_years += list(dc_data[col].dropna().dt.year.unique().astype(str))
        list_of_years = sorted(list(set(list_of_years)))

        self.record_dict.add('List_of_years', list_of_years, Record, 'Data')
        self.intermediate_results.List_of_years = list_of_years

    def _calculate_projectspecs(self):
        logger.info("Calculating project specs")
        results = calculate_projectspecs(self.transformed_data.df)

        # self.record_dict.add('HC_HPend', results.hc_hp_end_ratio_total, Record, 'Data')
        self.record_dict.add('HC_HPend_l', results.hc_hpend_ratio, Record, 'Data')
        # self.record_dict.add('Schouw_BIS', results.has_ready, Record, 'Data')
        # self.record_dict.add('HPend_l', results.homes_ended, Record, 'Data')
        # self.record_dict.add('HAS_werkvoorraad', results.werkvoorraad, Record, 'Data')

        # self.intermediate_results.HC_HPend = results.hc_hp_end_ratio_total
        self.intermediate_results.HC_HPend_l = results.hc_hpend_ratio
        # self.intermediate_results.Schouw_BIS = results.has_ready
        # self.intermediate_results.HPend_l = results.homes_ended
        # self.intermediate_results.HAS_werkvoorraad = results.werkvoorraad

    def _calculate_y_voorraad_act(self):
        logger.info("Calculating y voorraad act for KPN")
        y_voorraad_act = calculate_y_voorraad_act(self.transformed_data.df)
        self.intermediate_results.y_voorraad_act = y_voorraad_act
        self.record_dict.add('y_voorraad_act', y_voorraad_act, Record, 'Data')

    def _reden_na(self):
        logger.info("Calculating reden na graphs")
        overview_record = overview_reden_na(self.transformed_data.df, self.config['clusters_reden_na'])
        record_dict = individual_reden_na(self.transformed_data.df, self.config['clusters_reden_na'])
        self.record_dict.add('reden_na_overview', overview_record, Record, 'Data')
        self.record_dict.add('reden_na_projects', record_dict, DictRecord, 'Data')

    def _set_filters(self):
        self.record_dict.add("project_names", create_project_filter(self.transformed_data.df), ListRecord, "Data")

    # def _jaaroverzicht(self):
    #     # placeholder empty dict to shoot to firestore, to ensure no errors are thrown when no client specific logic has been made.
    #     jaaroverzicht = {}
    #     self.record_dict.add('jaaroverzicht', jaaroverzicht, Record, 'Data')

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
        self.record_dict.add('completed_status_counts', status_counts_dict, DictRecord, 'Data')

    def _calculate_redenna_per_period(self):
        logger.info("Calculating redenna per period (week & month)")
        by_week = calculate_redenna_per_period(df=self.transformed_data.df,
                                               date_column="hasdatum",
                                               freq="W-MON")
        self.record_dict.add('redenna_by_week', by_week, Record, 'Data')

        by_month = calculate_redenna_per_period(df=self.transformed_data.df,
                                                date_column="hasdatum",
                                                freq="M")
        self.record_dict.add('redenna_by_month', by_month, Record, 'Data')

        by_year = calculate_redenna_per_period(df=self.transformed_data.df,
                                               date_column="hasdatum",
                                               freq="Y")
        self.record_dict.add('redenna_by_year', by_year, Record, 'Data')

    def _make_records_for_dashboard_values(self, project_list):
        logger.info("Making records for dashboard overview  values")
        # Create a dictionary that contains the functions and the output name
        df = self.transformed_data.df
        function_dict = {'realisatie_bis': df[br.bis_opgeleverd_new(df)].status_civiel_datum,
                         'werkvoorraad_has': extract_werkvoorraad_has_dates(df),
                         'realisatie_hpend': extract_realisatie_hpend_dates(df),
                         'target': extract_target_dates(df=df,
                                                        project_list=project_list,
                                                        totals=self.transformed_data.get("totals"),
                                                        ftu=self.extracted_data.get("ftu")
                                                        ),
                         'voorspelling': extract_voorspelling_dates(
                             df=df,
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
                    # To remove the date when there is only one period (when summing over a year):
                    if len(record) == 1:
                        record = list(record.values())[0]
                    document_list.append(dict(
                        client=self.client,
                        graph_name=key,
                        frequency=freq,
                        year=year,
                        record=record
                    ))
        self.record_dict.add("Overzicht_per_jaar", document_list, DocumentListRecord, "Data",
                             document_key=["client", "graph_name", "frequency", "year"])

    def _make_records_of_voorspelling_and_planning_for_dashboard_values(self):
        logger.info("Making voorspelling and planning records for dashboard overview  values")
        # Create a dictionary that contains the functions and the output name
        function_dict = {'voorspelling_minus_HPend': extract_voorspelling_dates(
                                                df=self.transformed_data.df,
                                                ftu=self.extracted_data.get("ftu"),
                                                totals=self.transformed_data.get("totals")),
                         'planning_minus_HPend': extract_planning_dates(df=self.transformed_data.df,
                                                                        planning=self.transformed_data.get("planning"),
                                                                        client=self.client),
                         }
        realisatie_hpend = extract_realisatie_hpend_dates(self.transformed_data.df)
        list_of_freq = ['W-MON', 'M', 'Y']
        document_list = []
        for key, values in function_dict.items():
            for year in self.intermediate_results.List_of_years:
                for freq in list_of_freq:
                    record = voorspel_and_planning_minus_HPend_sum_over_periods_to_record(predicted=values,
                                                                                          realized=realisatie_hpend,
                                                                                          freq=freq, year=year)
                    # To remove the date when there is only one period (when summing over a year):
                    if len(record) == 1:
                        record = list(record.values())[0]
                    document_list.append(dict(
                        client=self.client,
                        graph_name=key,
                        frequency=freq,
                        year=year,
                        record=record
                    ))
        self.record_dict.add("Overzicht_voorspelling_planning_per_jaar", document_list, DocumentListRecord, "Data",
                             document_key=["client", "graph_name", "frequency", "year"])

    def _make_records_ratio_hc_hpend_for_dashboard_values(self):
        logger.info("Making record of ratio HC/HPend for dashboard overview  values")
        realisatie_hc = extract_realisatie_hc_dates(self.transformed_data.df)
        realisatie_hpend = extract_realisatie_hpend_dates(self.transformed_data.df)
        list_of_freq = ['W-MON', 'M', 'Y']
        document_list = []
        for year in self.intermediate_results.List_of_years:
            for freq in list_of_freq:
                record = ratio_sum_over_periods_to_record(numerator=realisatie_hc, divider=realisatie_hpend,
                                                          freq=freq, year=year)
                # To remove the date when there is only one period (when summing over a year):
                if len(record) == 1:
                    record = list(record.values())[0]
                document_list.append(dict(
                    client=self.client,
                    graph_name='ratio_hc_hpend',
                    frequency=freq,
                    year=year,
                    record=record
                ))
        self.record_dict.add("Ratios_hc_hpend_per_jaar", document_list, DocumentListRecord, "Data",
                             document_key=["client", "graph_name", "frequency", "year"])

    def _make_records_ratio_under_8weeks_for_dashboard_values(self):
        logger.info("Making record of ratio under 8 weeks/HPend for dashboard overview  values")
        realisatie_under_8weeks = self.transformed_data.df[br.on_time_opgeleverd(self.transformed_data.df)].opleverdatum
        realisatie_hpend = extract_realisatie_hpend_and_ordered_dates(self.transformed_data.df)
        list_of_freq = ['W-MON', 'M', 'Y']
        document_list = []
        for year in self.intermediate_results.List_of_years:
            for freq in list_of_freq:
                record = ratio_sum_over_periods_to_record(numerator=realisatie_under_8weeks, divider=realisatie_hpend,
                                                          freq=freq, year=year)
                # To remove the date when there is only one period (when summing over a year):
                if len(record) == 1:
                    record = list(record.values())[0]
                document_list.append(dict(
                    client=self.client,
                    graph_name='ratio_8weeks_hpend',
                    frequency=freq,
                    year=year,
                    record=record
                ))
        self.record_dict.add("Ratios_under_8weeks_per_jaar", document_list, DocumentListRecord, "Data",
                             document_key=["client", "graph_name", "frequency", "year"])

    def _make_intermediate_results_ratios_project_specific_values(self):
        logger.info("Making intermediate results of ratios for project specific values")
        # Create a dictionary that contains the functions and the output name
        df = self.transformed_data.df
        realisatie_hc = extract_realisatie_hc_dates(df=df, add_project_column=True)
        realisatie_hpend = extract_realisatie_hpend_dates(df=df, add_project_column=True)

        project_dict = {}
        for project in df.project.unique().tolist():
            record = self.calculate_ratio(project, realisatie_hc, realisatie_hpend)
            project_dict[project] = record

        self.intermediate_results.ratio_HC_HPend_per_project = project_dict

    def calculate_ratio(self, project, numerator, divider):
        project_dates_numerator = numerator[numerator.project == project].drop(labels='project', axis=1)
        project_dates_divider = divider[divider.project == project].drop(labels='project', axis=1)

        if len(project_dates_divider) == 0:
            record = 0
        else:
            record = len(project_dates_numerator) / len(project_dates_divider)
        return record


class FttXLoad(Load, FttXBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def load(self):
        logger.info("Loading documents...")
        self.record_dict.to_firestore()

    def load_enriched(self):
        pass


class FttXTestLoad(FttXLoad):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def load(self):
        logger.info("Nothing is loaded to the firestore as this is a test")
        logger.info("The following documents would have been updated/set:")
        for document in self.record_dict:
            logger.info(document.document_name())


class FttXETL(ETL, FttXExtract, FttXAnalyse, FttXTransform, FttXLoad):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def perform(self):
        self.extract()
        self.transform()
        self.analyse()
        self.load()

    def document_names(self):
        return [value.document_name(client=self.client, graph_name=key) for key, value in self.record_dict.items()]

    def __repr__(self):
        return f"Analysis(client={self.client})"

    def __str__(self):
        fields = [field_name for field_name, data in self.record_dict.items()]
        return f"Analysis(client={self.client}) containing: {fields}"

    def _repr_html_(self):
        rows = "\n".join(
            [data.to_table_part(field_name, self.client)
             for field_name, data in self.record_dict.items()
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


class FttXLocalETL(PickleExtract, FttXETL):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def load(self):
        if 'FIRESTORE_EMULATOR_HOST' in os.environ:
            logger.info("Loading into emulated firestore")
            super().load()
        else:
            logger.warning(
                "Attempting to load with a local ETL process but no emulator is configured. Loading aborted.")
