import pandas as pd
import numpy as np
from google.cloud import firestore, storage
import os
import time
import unicodedata


class ETL_target_data():

    def __init__(self, target_document, initialise=False):
        self.target_document = target_document

    # TODO where does the data come from?
    def extract(self):
        doc = firestore.Client().collection('Graphs').document('analysis').get().to_dict()
        self.FTU0 = doc['FTU0']
        self.FTU1 = doc['FTU1']

    def transform(self):
        pass


class ETL_planning_data():

    def __init__(self, location):
        self.location = location

    def extract(self):
        if 'gs://' in self.location:
            xls = pd.ExcelFile(self.location)
        else:
            xls = pd.ExcelFile(self.location + 'Data_20200101_extra/Forecast JUNI 2020_def.xlsx')
        df = pd.read_excel(xls, 'FTTX ').fillna(0)
        self.data = df

    def transform(self):
        HP = dict(HPendT=[0] * 52)
        df = self.data
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
        self.data = df


class ETL_project_data():

    def __init__(self, bucket, projects, col):
        self.bucket = bucket
        self.projects = projects
        self.columns = col

    def extract(self, location):
        bucket = storage.Client().get_bucket(self.bucket)
        blobs = bucket.list_blobs()
        for blob in blobs:
            if pd.Timestamp.now().strftime('%Y%m%d') in blob.name:
                blob.download_to_filename(location+'jsonFC/' + blob.name.split('/')[-1])
        files = os.listdir(location+'jsonFC/')
        self.data = make_frame_dict(files, location+'jsonFC/', self.projects)

    def transform(self, flag=1):
        transformed_data = {}
        for project, df in self.data.items():
            df = self.rename_columns(df)
            if flag == 0:
                df = df[self.columns]
            df = self.set_hasdatum(df)
            df = self.set_opleverdatum(df)
            print(project, len(df))
            if (project in self.projects) and (project not in transformed_data.keys()):
                transformed_data[project] = df
            if (project in self.projects) and (project in transformed_data.keys()):
                transformed_data[project] = transformed_data[project].append(df, ignore_index=True)
                transformed_data[project] = transformed_data[project].drop_duplicates(keep='first')
                # generate this as error output?
            # Hope this doesn't do anything anymore. Really weird fix.
            # if project not in ['Brielle', 'Helvoirt POP Volbouw']:  # zitten in ingest folder 20200622
            #     os.remove(path_data + '../jsonFC/' + fn)
        self.data = transformed_data
        # # I don't understand the flag variable. Does it do anything? Function is always called with flag 0
        # if flag == 0:
        #     for key in self.data:
        #         self.data[project].sleutel = [hashlib.sha256(el.encode()).hexdigest() for el in self.data[key].sleutel]

        # for key in self.projects:
        #     if key not in self.data:
        #         self.data[project] = pd.DataFrame(columns=self.columns)

    def set_opleverdatum(self, df):
        df.loc[~df['opleverdatum'].isna(), ('opleverdatum')] =\
            [el[6:10] + '-' + el[3:5] + '-' + el[0:2] for el in df[~df['opleverdatum'].isna()]['opleverdatum']]
        return df

    def set_hasdatum(self, df):
        df.loc[~df['hasdatum'].isna(), ('hasdatum')] =\
            [el[6:10] + '-' + el[3:5] + '-' + el[0:2] for el in df[~df['hasdatum'].isna()]['hasdatum']]
        return df

    def rename_columns(self, df):
        df.rename(columns={
                            'Sleutel': 'sleutel', 'Soort_bouw': 'soort_bouw',
                            'LaswerkAPGereed': 'laswerkapgereed', 'LaswerkDPGereed': 'laswerkdpgereed',
                            'Opleverdatum': 'opleverdatum', 'Opleverstatus': 'opleverstatus',
                            'RedenNA': 'redenna', 'X locatie Rol': 'x_locatie_rol',
                            'Y locatie Rol': 'y_locatie_rol', 'X locatie DP': 'x_locatie_dp',
                            'Y locatie DP': 'y_locatie_dp', 'Toestemming': 'toestemming',
                            'HASdatum': 'hasdatum', 'title': 'project', 'KabelID': 'kabelid',
                            'Postcode': 'postcode', 'Huisnummer': 'huisnummer', 'Adres': 'adres',
                            'Plandatum': 'plandatum', 'FTU_type': 'ftu_type', 'Toelichting status': 'toelichting_status',
                            'Kast': 'kast', 'Kastrij': 'kastrij', 'ODF': 'odf', 'ODFpos': 'odfpos',
                            'CATVpos': 'catvpos', 'CATV': 'catv', 'Areapop': 'areapop', 'ProjectCode': 'projectcode',
                            'SchouwDatum': 'schouwdatum'}, inplace=True)
        return df


class ETL_project_data_database(ETL_project_data):

    def extract(self):
        t = time.time()
        df_l = {}
        for key in self.projects:
            docs = firestore.Client().collection('Projects').where('project', '==', key).stream()
            records = []
            for doc in docs:
                records += [doc.to_dict()]
            if records != []:
                df_l[key] = pd.DataFrame(records)[self.columns].fillna(np.nan)
                print(f"Record: {len(df_l[key])}")
            else:
                df_l[key] = pd.DataFrame(columns=self.columns).fillna(np.nan)
            # to correct for datetime value at HUB
            df_l[key].loc[~df_l[key]['opleverdatum'].isna(), ('opleverdatum')] = \
                [el[0:10] for el in df_l[key][~df_l[key]['opleverdatum'].isna()]['opleverdatum']]
            df_l[key].loc[~df_l[key]['hasdatum'].isna(), ('hasdatum')] = \
                [el[0:10] for el in df_l[key][~df_l[key]['hasdatum'].isna()]['hasdatum']]

            print(key)
            print('Time: ' + str((time.time() - t)/60) + ' minutes')
        self.data = df_l

    def transform(self):
        self.data = self.data


def make_frame_dict(files, source, projects):
    dataframe = pd.DataFrame()
    for filename in files:
        if filename[-5:] == '.json':
            df = pd.DataFrame(pd.read_json(source + filename, orient='records')['data'].to_list())
            titel = df['title'].iloc[0][0:-13]
            if titel in projects:
                df['project'] = unicodedata.normalize("NFKD", titel)
                dataframe = dataframe.append(df)
    df_dict = {}
    for project in dataframe.project.unique():
        df_dict[project] = dataframe[dataframe.project == project]

    return df_dict
