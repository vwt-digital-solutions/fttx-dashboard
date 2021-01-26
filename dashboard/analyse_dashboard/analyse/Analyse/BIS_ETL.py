from google.cloud import storage

from Analyse.ETL import Extract, ETL, Transform

import pandas as pd
import re
import config


class BISExtract(Extract):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def extract(self):
        '''
        Extract all data from BIS Excel
        '''
        print("Extracting data from Excel")
        df_list = []
        client = storage.Client()
        bucket = config.data_bucket
        folder = config.folder_data_schaderapportages
        for file in client.list_blobs(bucket, prefix=folder):
            filename = file.name
            if filename[-5:] == '.xlsx':
                file_path = f'gs://{bucket}/{file.name}'
                df = pd.read_excel(file_path,
                                   sheet_name='Productie',
                                   skiprows=list(range(0, 12)))
                # Dirty regex to find project from filename. Should enforce standardises names when files will be
                # updated.
                project = re.findall(r"schaderapportages/B\d{4} (.*) (?=Invulformulier|invulformulier)",
                                     filename)[0]
                df['project'] = project
                df_list.append(df)

        df = pd.concat(df_list)

        self.extracted_data.df = df


class BISTransform(Transform):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def transform(self, **kwargs):
        super().transform()
        print("Transforming the data to create workable pd DataFrame")
        self._rename_columns()
        self._expand_dates()
        self._set_totals()

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

    def _set_totals(self):
        self.transformed_data.totals = {}
        self.transformed_data.totals['BIS geul'] = {'KPN Spijkernisse': 70166}
        self.transformed_data.totals['tuinboringen'] = {'KPN Spijkernisse': 31826}

    def _expand_dates(self):
        print('Expanding dates to create date-based index')

        def transform_weeknumbers(x):
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


class BISETL(ETL, BISExtract, BISTransform):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def perform(self):
        self.extract()
        self.transform()
