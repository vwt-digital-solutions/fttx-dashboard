from Analyse.ETL import Extract, ETL, Transform

import pandas as pd

from config import path_to_bis_excel


class BISExtract(Extract):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def extract(self):
        '''
        Extract all data from BIS Excel
        '''
        print("Extracting data from Excel")
        self.excel_path = path_to_bis_excel
        df = pd.read_excel(self.excel_path, sheet_name='Productie', skiprows=list(range(0, 12)))

        self.extracted_data.df = df


class BISTransform(Transform):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def transform(self, **kwargs):
        super().transform()
        print("Transforming the data to create workable pd DataFrame")
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

    def _expand_dates(self):
        print('Expanding dates to create date-based index')

        def transform_weeknumbers(x):
            if x.startswith('2021_'):
                return pd.to_datetime(x + '1', format='%Y_%W%w')
            else:
                return (pd.to_datetime(x + '1', format='%Y_%W%w')) - pd.to_timedelta(7, unit='d')

        self.transformed_data.df['date'] = self.transformed_data.df['date'].apply(transform_weeknumbers)
        self.transformed_data.df = self.transformed_data.df.set_index('date')

        df_Date = pd.date_range(start=self.transformed_data.df.index.min(),
                                end=(self.transformed_data.df.index.max() + pd.to_timedelta(6, unit='d')),
                                freq='D')
        self.transformed_data.df = self.transformed_data.df.reindex(df_Date, fill_value=None)


class BISETL(ETL, BISExtract, BISTransform):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def perform(self):
        self.extract()
        self.transform()
