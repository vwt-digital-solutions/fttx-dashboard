import os
import time
from google.cloud import firestore

from Analyse.Data import Data
from Analyse.ETL import Extract, ETL, Transform, ETLBase, Load
import pandas as pd
import numpy as np
import pickle  # nosec

import logging

from Analyse.Record import RecordDict, Record, DictRecord, ListRecord, DocumentListRecord
import business_rules as br
from functions import calculate_projectspecs, overview_reden_na, individual_reden_na, set_filters, \
    calculate_redenna_per_period, rules_to_state, calculate_y_voorraad_act, cluster_reden_na, get_database_engine, \
    sum_over_period
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
        self.record_dict = RecordDict()
        self.intermediate_results = Data()


class FttXExtract(Extract):

    def __init__(self, **kwargs):
        if not hasattr(self, 'config'):
            self.config = kwargs.get("config")
        if not self.config:
            raise ValueError("No config provided in init")
        self.projects = self.config["projects"]
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
        sql = f"""
select fca.*
from fc_aansluitingen fca
inner join fc_client_project_map cpm on fca.project = cpm.project
where cpm.client = '{self.config.get("name")}'
"""  # nosec
        df = pd.read_sql(sql, get_database_engine())
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
        self._add_columns()
        self._cluster_reden_na()
        self._add_status_columns()
        self._set_totals()

    def _set_totals(self):
        self.transformed_data.totals = {}
        for project in self.transformed_data.df.project.unique():
            self.transformed_data.totals[project] = len(
                self.transformed_data.df[self.transformed_data.df['project'] == project])

    def _fix_dates(self):
        logger.info("Changing columns to datetime column if there is 'datum' in column name.")
        datums = [col for col in self.transformed_data.df.columns if "datum" in col]
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


class FttXAnalyse(FttXBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not hasattr(self, 'config'):
            self.config = kwargs.get("config")
        self.record_dict = RecordDict()
        self.intermediate_results = Data()

    def analyse(self):
        logger.info("Analysing using the FttX protocol")
        if toggles.new_structure_overviews:
            self._make_records_realisatie_bis
            self._calculate_list_of_years()
        self._calculate_projectspecs()
        self._calculate_y_voorraad_act()
        self._reden_na()
        self._set_filters()
        self._calculate_status_counts_per_project()
        self._calculate_redenna_per_period()
        self._jaaroverzicht()
        self._progress_per_phase()
        self._progress_per_phase_over_time()

    def _progress_per_phase_over_time(self):
        logger.info("Calculating project progress per phase over time")
        document_list = []
        for project, df in self.transformed_data.df.groupby("project"):
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
        date_columns = [col for col in self.transformed_data.df.columns if "datum" in col or "date" in col]
        dc_data = self.transformed_data.df.loc[:, date_columns]
        list_of_years = []
        for col in dc_data.columns:
            list_of_years += list(dc_data[col].dropna().dt.year.unique())
        list_of_years = sorted(list(set(list_of_years)))

        self.record_dict.add('List_of_years', list_of_years, Record, 'Data')
        self.intermediate_results.List_of_years = list_of_years

    def _calculate_projectspecs(self):
        logger.info("Calculating project specs")
        results = calculate_projectspecs(self.transformed_data.df)

        self.record_dict.add('HC_HPend', results.hc_hp_end_ratio_total, Record, 'Data')
        self.record_dict.add('HC_HPend_l', results.hc_hpend_ratio, Record, 'Data')
        self.record_dict.add('Schouw_BIS', results.has_ready, Record, 'Data')
        self.record_dict.add('HPend_l', results.homes_ended, Record, 'Data')
        self.record_dict.add('HAS_werkvoorraad', results.werkvoorraad, Record, 'Data')

        self.intermediate_results.HC_HPend = results.hc_hp_end_ratio_total
        self.intermediate_results.HC_HPend_l = results.hc_hpend_ratio
        self.intermediate_results.Schouw_BIS = results.has_ready
        self.intermediate_results.HPend_l = results.homes_ended
        self.intermediate_results.HAS_werkvoorraad = results.werkvoorraad

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
        self.record_dict.add("project_names", set_filters(self.transformed_data.df), ListRecord, "Data")

    def _jaaroverzicht(self):
        # placeholder empty dict to shoot to firestore, to ensure no errors are thrown when no client specific logic has been made.
        jaaroverzicht = {}
        self.record_dict.add('jaaroverzicht', jaaroverzicht, Record, 'Data')

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
        by_week = calculate_redenna_per_period(self.transformed_data.df,
                                               date_column="hasdatum",
                                               freq="W-MON")
        self.record_dict.add('redenna_by_week', by_week, Record, 'Data')

        by_month = calculate_redenna_per_period(self.transformed_data.df,
                                                date_column="hasdatum",
                                                freq="MS")
        self.record_dict.add('redenna_by_month', by_month, Record, 'Data')

    def _make_records_realisatie_bis():
        record = sum_over_period(ds=pd.Series(), freq='W-MON', year='2020')
        print(record)


class FttXLoad(Load, FttXBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def load(self):
        logger.info("Loading documents...")
        self.record_dict.to_firestore(self.client)

    def load_enriched(self):
        pass


class FttXTestLoad(FttXLoad):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def load(self):
        logger.info("Nothing is loaded to the firestore as this is a test")
        logger.info("The following documents would have been updated/set:")
        for document in self.record_dict:
            logger.info(self.record_dict[document].document_name(client=self.client,
                                                                 graph_name=document,
                                                                 document=None))


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
