import time
from google.cloud import firestore

from Analyse.Data import Data
from Analyse.ETL import Extract, ETL, Transform, ETLBase, Load
import pandas as pd
import numpy as np
import pickle  # nosec

import logging

from Record import RecordDict, Record, DictRecord
from functions import calculate_projectspecs, overview_reden_na, individual_reden_na

logger = logging.getLogger('FttX Analyse')


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
        df = pd.DataFrame([])
        for key in self.projects:
            start_time = time.time()
            logger.debug(f"Extracting {key}...")
            docs = firestore.Client().collection('Projects').where('project', '==', key).stream()
            new_records = [doc.to_dict() for doc in docs]
            df.append(pd.DataFrame(new_records).fillna(np.nan), ignore_index=True)
            logger.debug(f"Extracted {len(new_records)} records in {time.time() - start_time} seconds")

        projects_category = pd.CategoricalDtype(categories=self.projects)
        df['project'] = df.project.astype(projects_category)

        self.extracted_data.df = df


class PickleExtract(Extract, FttXBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def extract(self):
        logger.info("Extracting data, trying to use a pickle")
        pickle_name = f"{self.client}_data.pickle"
        try:
            self.extracted_data = pickle.load(open(pickle_name, "rb"))  # nosec
            logger.debug("Extracted data from pickle")
        except (OSError, IOError, FileNotFoundError):
            logger.debug(f"{pickle_name} not available, using fallback and pickling the result")
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

    def _fix_dates(self):
        logger.debug("Changing columns to datetime column if there is 'datum' in column name.")
        datums = [col for col in self.transformed_data.df.columns if "datum" in col]
        self.transformed_data.df[datums] = self.transformed_data.df[datums].apply(pd.to_datetime,
                                                                                  infer_datetime_format=True,
                                                                                  errors="coerce")

    def _add_columns(self):
        logger.debug("Adding columns to dataframe")
        start_year = pd.to_datetime(self.year + '-01-01')
        end_year = pd.to_datetime(self.year + '-12-31')

        self.transformed_data.df['hpend'] = self.transformed_data.df.opleverdatum.apply(
            lambda x: (x >= start_year) and (x <= end_year))
        self.transformed_data.df['homes_completed'] = (self.transformed_data.df.opleverstatus == '2') & (
            self.transformed_data.df.hpend)
        self.transformed_data.df['homes_completed_total'] = (self.transformed_data.df.opleverstatus == '2')
        self.transformed_data.df['bis_gereed'] = self.transformed_data.df.opleverstatus != '0'
        self.transformed_data.df['in_has_werkvoorraad'] = (
                (~self.transformed_data.df.toestemming.isna()) &
                (self.transformed_data.df.opleverstatus != '0') &
                (self.transformed_data.df.opleverdatum.isna())
        )


class FttXAnalyse(FttXBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not hasattr(self, 'config'):
            self.config = kwargs.get("config")
        self.record_dict = RecordDict()
        self.intermediate_results = Data()

    def analyse(self):
        logger.info("Analysing using the FttX protocol")
        self._calculate_projectspecs()
        self._reden_na()

    def _calculate_projectspecs(self):
        logger.debug("Calculating project specs")
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

    def _reden_na(self):
        logger.debug("Calculating reden na graphs")
        overview_record = overview_reden_na(self.transformed_data.df, self.config['clusters_reden_na'])
        record_dict = individual_reden_na(self.transformed_data.df, self.config['clusters_reden_na'])
        self.record_dict.add('reden_na_overview', overview_record, Record, 'Data')
        self.record_dict.add('reden_na_projects', record_dict, DictRecord, 'Data')


class FttXLoad(Load, FttXBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def load(self):
        self.record_dict.to_firestore(self.client)


class FttXTestLoad(FttXLoad):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def load(self):
        logger.info("Nothing is loaded to the firestore as this is a test")
        logger.debug("The following documents would have been updated/set:")
        for document in self.record_dict:
            logger.debug(self.record_dict[document].document_name(client=self.client, graph_name=document, document=None))


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
