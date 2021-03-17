from google.cloud import storage

from Analyse.ETL import Extract, ETL, Transform, logger
from functions import get_map_bnumber_vs_project_from_sql

import pandas as pd
import re
import config


class BISExtract(Extract):
    """
    Class that extracts meter data and adds them as attribute to the ETL class.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def extract(self):
        """
        Extracts data for meters graven from Excels on the google cloud, and adds them tot he extracted data attribute
        """
        print("Extracting data from Excel")
        df_list = []
        client = storage.Client()
        bucket = config.data_bucket
        folder = config.folder_data_schaderapportages
        mapping = get_map_bnumber_vs_project_from_sql()
        for file in client.list_blobs(bucket, prefix=folder):
            filename = file.name
            if filename[-5:] == '.xlsx':
                file_path = f'gs://{bucket}/{file.name}'
                df = pd.read_excel(file_path,
                                   sheet_name='Productie',
                                   skiprows=list(range(0, 12)))
                b_number = re.findall(r"B\d*", filename)[0][1:]  # find b-number (B + fiberconnect project number)
                if b_number in list(mapping.index):
                    df['project'] = mapping.at[b_number, 'project']
                    df_list.append(df)
                else:
                    logger.error(f'Cannot map b-number to project name: {b_number}')

        df = pd.concat(df_list, sort=True)

        self.extracted_data.df = df


class BISTransform(Transform):
    """
    Performs necessary transformation steps on extracted graven data and adds them to transformed data
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def transform(self, **kwargs):
        """
        Wrapper function to perform all transformation steps for meters.
        """
        super().transform()
        logger.info("Transforming the data to create workable pd DataFrame")
        self._rename_columns()
        self._expand_dates()

    def _rename_columns(self):
        df_renamed = pd.DataFrame()
        df_renamed = df_renamed.append(self.extracted_data.df, ignore_index=True)
        df_renamed = df_renamed.rename(columns={
                                'Kalender weeknummer': 'date',
                                '# Meters BIS geul': 'meters_bis_geul',
                                '# Meters tuinboringen': 'meters_tuinboring',
                                '# Huisaansluitingen': 'aantal_has',
                                '# BIS ploegen': 'aantal_bis_ploegen',
                                '# Tuinploegen': 'aantal_tuin_ploegen',
                                '# HAS ploegen': 'aantal_has_ploegen',
                                'Bijzonderheden': 'bijzonderheden'})

        self.transformed_data.df = df_renamed

    # TODO: Documentation by Casper van Houten
    def _expand_dates(self):
        """
        Expands dataframe with to be a timeseries of the complete daterange between first and last date,
        filling missing dates with zeroes.
        """
        logger.info('Expanding dates to create date-based index')

        # TODO: Documentation by Casper van Houten.
        # TODO: Remove hardcoded year.
        def transform_weeknumbers(x):
            """
            Transforms input date into a datetime object
            Args:
                x: input date

            Returns: datetime object with the first date of the input week.

            """
            if x.startswith('2021_'):
                return pd.to_datetime(x + '1', format='%Y_%W%w')
            else:
                return (pd.to_datetime(x + '1', format='%Y_%W%w')) - pd.to_timedelta(7, unit='d')

        self.transformed_data.df['date'] = self.transformed_data.df['date'].apply(transform_weeknumbers)
        self.transformed_data.df = self.transformed_data.df.set_index(['project', 'date'])

        df_date = pd.date_range(start=self.transformed_data.df.index.get_level_values(1).min(),
                                end=(self.transformed_data.df.index.get_level_values(1).max() +
                                     pd.to_timedelta(6, unit='d')),
                                freq='D')
        self.transformed_data.df = self.transformed_data.df.reindex(df_date, fill_value=None, level=1)


# TODO: Documentation by Casper van Houten
class BISETL(ETL, BISExtract, BISTransform):
    """
    ETL for graven meters.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    # TODO: Documentation by Casper van Houten
    def perform(self):
        """
        Performs extract and transform for bis etl, which are all ETL steps associated with BIS
        """
        self.extract()
        self.transform()
