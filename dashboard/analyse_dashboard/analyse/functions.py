import pandas as pd
import numpy as np
from google.cloud import firestore, storage
import os
import time
import json
import datetime
import hashlib
import unicodedata


def make_frame_dict(files, source):
    dataframe = pd.DataFrame()
    for filename in files:
        df = pd.DataFrame(pd.read_json(source + filename, orient='records')['data'].to_list())
        df['project'] = unicodedata.normalize("NFKD", df['title'].iloc[0][0:-13])
        dataframe = dataframe.append(df)
    df_dict = {}
    for project in df.project.unique():
        df_dict[project] = dataframe[dataframe.project == project]

    return df_dict


class Customer():

    def __init__(self, config):
        for key, value in config.items():
            setattr(self, key, value)

    def set_etl_processes(self):
        self.etl_project_data = ETL_project_data_database
        self.etl_planning_data = ETL_planning_data
        self.etl_target_data = ETL_target_data

    def get_data(self):
        etl = self.etl_project_data(self.bucket, self.projects, self.columns)
        etl.extract()
        etl.transform()
        return etl.data

    def get_data_planning(self):
        etl = self.etl_planning_data(self.planning_location)
        etl.extract()
        etl.transform()
        return etl.data

    def get_data_targets(self):
        etl = self.etl_target_data(self.target_document)
        etl.extract()
        etl.transform()
        self. etl.FTU0, etl.FTU1

    def get_source_data(self):
        self.set_etl_processes()
        self.get_data()
        self.get_data_planning()
        self.get_data_targets()


class Customer_tmobile(Customer):

    def set_etl_processes(self):
        self.etl_project_data = ETL_project_data

    def get_data(self, local_file=None):
        etl = self.etl_project_data(self.bucket, self.projects, self.columns)
        etl.extract(local_file)
        etl.transform()
        return etl.data


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
        self.data = make_frame_dict(files, location+'jsonFC/')

    def transform(self, flag=0):
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


def get_data_from_ingestbucket(gpath_i, col, path_data, subset, flag):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gpath_i
    fn_l = os.listdir(path_data + '../jsonFC/')
    client = storage.Client()
    bucket = client.get_bucket('vwt-d-gew1-it-fiberconnect-int-preprocess-stg')
    blobs = bucket.list_blobs()
    for blob in blobs:
        if pd.Timestamp.now().strftime('%Y%m%d') in blob.name:
            blob.download_to_filename(path_data + '../jsonFC/' + blob.name.split('/')[-1])
    fn_l = os.listdir(path_data + '../jsonFC/')

    df_l = {}
    for fn in fn_l:
        df = pd.DataFrame(pd.read_json(path_data + '../jsonFC/' + fn, orient='records')['data'].to_list())
        df = df.replace('', np.nan).fillna(np.nan)
        if df['title'].iloc[0][0:-13] != 'Bergen op Zoom Noord\xa0  wijk 01\xa0+ Halsteren':
            df['title'] = key = df['title'].iloc[0][0:-13]
        else:
            df['title'] = key = 'Bergen op Zoom Noord en Halsteren'
        # df = df[~df.sleutel.isna()]  # generate this as error output?
        df.rename(columns={'Sleutel': 'sleutel', 'Soort_bouw': 'soort_bouw',
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
        if flag == 0:
            df = df[col]
        df.loc[~df['opleverdatum'].isna(), ('opleverdatum')] =\
            [el[6:10] + '-' + el[3:5] + '-' + el[0:2] for el in df[~df['opleverdatum'].isna()]['opleverdatum']]
        df.loc[~df['hasdatum'].isna(), ('hasdatum')] =\
            [el[6:10] + '-' + el[3:5] + '-' + el[0:2] for el in df[~df['hasdatum'].isna()]['hasdatum']]
        if (key in subset) and (key not in df_l.keys()):
            df_l[key] = df
        if (key in subset) and (key in df_l.keys()):
            df_l[key] = df_l[key].append(df, ignore_index=True)
            df_l[key] = df_l[key].drop_duplicates(keep='first')  # generate this as error output?

        if key not in ['Brielle', 'Helvoirt POP Volbouw']:  # zitten in ingest folder 20200622
            os.remove(path_data + '../jsonFC/' + fn)

    # hash sleutel code
    if flag == 0:
        for key in df_l:
            df_l[key].sleutel = [hashlib.sha256(el.encode()).hexdigest() for el in df_l[key].sleutel]

    for key in subset:
        if key not in df_l:
            df_l[key] = pd.DataFrame(columns=col)

    return df_l


class Record():
    def __init__(self, record, collection):
        self.record = None
        self.transform(record)
        self.collection = collection

    def transform(self, record):
        self.record = record

    def to_firestore(self, graph_name, client):
        document = firestore.Client().collection(self.collection).document(graph_name)
        document.set(dict(record=self.record,
                     client=client,
                     graph_name=graph_name)
                     )


class IntRecord(Record):

    def transform(self, record):
        self.record = [int(el) for el in record]


class DateRecord(Record):

    def transform(self, record):
        self.record = [el.strftime('%Y-%m-%d') for el in record]


class ListRecord(Record):

    def transform(self, record):
        self.record = {}
        for k, v in record.items():
            self.record[k] = list(v)


class StringRecord(Record):
    def transform(self, record):
        self.record = {}
        for k, v in record.items():
            self.record[k] = str(v)


class RecordDict(Record):
    def to_firestore(self, graph_name, client):
        for k, v in self.record.items():
            document = firestore.Client().collection(self.collection).document(k)
            document.set(dict(record=self.record,
                              client=client,
                              graph_name=k))


class Analysis():

    def __init__(self, client):
        self._client = client

    def set_input_fields(self, date_FTU0, date_FTU1, x_d):
        self.date_FTU0 = Record(date_FTU0, collection='Data')
        self.date_FTU1 = Record(date_FTU1, collection='Data')
        self.x_d = DateRecord(x_d, collection="Data")

    def prognose(self, df_l, start_time, timeline, total_objects, date_FTU0):
        print("Prognose")
        results = prognose(df_l, start_time, timeline, total_objects, date_FTU0)
        self.rc1 = ListRecord(results[0], collection='Data')
        self.rc2 = ListRecord(results[1], collection='Data')
        d_real_l_r = {k: v["Aantal"] for k, v in results[2].items()}
        self.d_real_l_r = ListRecord(d_real_l_r, collection="Data")
        d_real_l_ri = {k: v.index for k, v in results[2].items()}
        self.d_real_l_ri = ListRecord(d_real_l_ri, collection="Data")
        self.y_prog_l = ListRecord(results[3], collection='Data')
        self.x_prog = IntRecord(results[4], collection='Data')
        self.t_shift = StringRecord(results[5], collection='Data')
        self.cutoff = Record(results[6], collection='Data')
        return results

    def targets(self, x_prog, timeline, t_shift, date_FTU0, date_FTU1, rc1, d_real_l):
        print("Targets")
        results = targets(x_prog, timeline, t_shift, date_FTU0, date_FTU1, rc1, d_real_l)
        self.y_target_l = ListRecord(results[0], collection='Data')
        return results

    def error_check_FCBC(self, df_l):
        print("Error check")
        results = error_check_FCBC(df_l)
        self.n_err = Record(results[0], collection='Data')
        self.errors_FC_BC = Record(results[1], collection='Data')
        print("error check done")
        return results

    def calculate_projectspecs(self, df_l):
        print("Projectspecs")
        results = calculate_projectspecs(df_l)
        self.HC_HPend = Record(results[0], collection='Data')
        self.HC_HPend_l = Record(results[1], collection='Data')
        self.Schouw_BIS = Record(results[2], collection='Data')
        self.HPend_l = Record(results[3], collection='Data')
        self.HAS_werkvoorraad = Record(results[4], collection='Data')
        return results

    def calculate_graph_overview(self, df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad):
        graph_targets_W = graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad, res='W-MON')
        graph_targets_M, jaaroverzicht = graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad, res='M')
        self.graph_targets_W = Record(graph_targets_W, collection='Graphs')
        self.graph_targets_M = Record(graph_targets_M, collection='Graphs')
        self.jaaroverzicht = Record(jaaroverzicht, collection='Data')

    def performance_matrix(self, x_d, y_target_l, d_real_l, tot_l, t_diff, y_voorraad_act):
        graph = performance_matrix(x_d, y_target_l, d_real_l, tot_l, t_diff, y_voorraad_act)
        self.project_performance = Record(graph, collection="Graphs")

    def calculate_y_voorraad_act(self, df_l):
        results = calculate_y_voorraad_act(df_l)
        self.y_voorraad_act = Record(results, collection='Data')
        return results

    def prognose_graph(self, x_d, y_prog_l, d_real_l, y_target_l):
        result_dict = prognose_graph(x_d, y_prog_l, d_real_l, y_target_l)
        self.prognose_graph_dict = RecordDict(result_dict, collection="Graphs")

    def info_table(self, tot_l, d_real_l, HP, y_target_l, x_d, HC_HPend_l, Schouw_BIS, HPend_l, n_err):
        record = info_table(tot_l, d_real_l, HP, y_target_l, x_d, HC_HPend_l, Schouw_BIS, HPend_l, n_err)
        self.info_table = Record(record, collection="Graphs")

    def reden_na(self, df_l, clusters):
        overview_record = overview_reden_na(df_l, clusters)
        record_dict = individual_reden_na(df_l, clusters)
        self.reden_na_overview = Record(overview_record, collection="Graphs")
        self.reden_na_projects = RecordDict(record_dict, collection="Graphs")

    def to_firestore(self):
        for field_name, data in self.__dict__.items():
            if not field_name[0] == "_":
                try:
                    data.to_firestore(field_name, self._client)
                    print(f"Wrote {field_name} to firestore")
                except TypeError:
                    print(f"Could not write {field_name} to firestore.")


def extract_data_planning(path_data):
    if 'gs://' in path_data:
        xls = pd.ExcelFile(path_data)
    else:
        xls = pd.ExcelFile(path_data + 'Data_20200101_extra/Forecast JUNI 2020_def.xlsx')
    df = pd.read_excel(xls, 'FTTX ').fillna(0)
    return df


# TODO: Transform this function to use more elegant pandas ETL-style process
def transform_data_planning(df):
    HP = dict(HPendT=[0] * 52)
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
    return HP


def get_data_planning(path_data, subset_KPN_2020):
    df = extract_data_planning(path_data)
    HP = transform_data_planning(df)
    return HP


def get_data_targets(path_data):
    doc = firestore.Client().collection('Graphs').document('analysis').get().to_dict()
    date_FTU0 = doc['FTU0']
    date_FTU1 = doc['FTU1']
    return date_FTU0, date_FTU1


# Function to use only when data_targets in database need to be reset.
# TODO: Create function structure that can reinitialise the database, partially as well as completely.
def get_data_targets_init(path_data):
    map_key2 = {
        # FT0 en FT1
        'Arnhem Klarendal': 'Arnhem Klarendal',
        'Arnhem Gulden Bodem Schaarsbergen': 'Arnhem Gulden Bodem Schaarsbergen',
        'Breda Tuinzicht': 'Breda Tuinzicht',
        'Breda Brabantpark': 'Breda Brabantpark',
        'Bergen op Zoom Oost': 'Bergen op Zoom Oost',
        'Bergen op Zoom Oude Stad + West wijk 03': 'Bergen op Zoom oude stad',
        'Nijmegen Oosterhout': 'Nijmegen Oosterhout',
        'Nijmegen centrum Bottendaal': 'Nijmegen Bottendaal',
        'Nijmegen Biezen Wolfskuil Hatert': 'Nijmegen Biezen-Wolfskuil-Hatert ',
        'Den Haag-Wijk 34 Eskamp-Morgenstond-West': 'Den Haag Morgenstond west',
        'Spijkenisse': 'KPN Spijkernisse',
        'Gouda Centrum': 'Gouda Centrum',  # niet in FC, ?? waar is deze
        # FT0 in 2020 --> eind datum schatten
        'Bergen op Zoom Noord  wijk 01 + Halsteren': 'Bergen op Zoom Noord en Halsteren',  # niet in FC
        'Nijmegen Dukenburg': 'Nijmegen Dukenburg',  # niet in FC
        'Den Haag - Haagse Hout-Bezuidenhout West': 'Den Haag - Haagse Hout-Bezuidenhout West',  # niet in FC??
        'Den Haag - Vrederust en Bouwlust': 'Den Haag - Vrederust en Bouwlust',  # niet in FC??
        'Gouda Kort Haarlem en Noord': 'KPN Gouda Kort Haarlem en Noord',
        # wel in FC, geen FT0 of FT1, niet afgerond, niet actief in FC...
        # Den Haag Cluster B (geen KPN), Den Haag Regentessekwatier (ON HOLD), Den Haag (??)
        # afgerond in FC...FTU0/FTU1 schatten
        # Arnhem Marlburgen, Arnhem Spijkerbuurt, Bavel, Brielle, Helvoirt, LCM project
    }
    fn_targets = 'Data_20200101_extra/20200501_Overzicht bouwstromen KPN met indiendata offerte v12.xlsx'
    df_targetsKPN = pd.read_excel(path_data + fn_targets, sheet_name='KPN')
    date_FTU0 = {}
    date_FTU1 = {}
    for i, key in enumerate(df_targetsKPN['d.d. 01-05-2020 v11']):
        if key in map_key2:
            if not pd.isnull(df_targetsKPN.loc[i, '1e FTU']):
                date_FTU0[map_key2[key]] = df_targetsKPN.loc[i, '1e FTU'].strftime('%Y-%m-%d')
            if (not pd.isnull(df_targetsKPN.loc[i, 'Laatste FTU'])) & (df_targetsKPN.loc[i, 'Laatste FTU'] != '?'):
                date_FTU1[map_key2[key]] = df_targetsKPN.loc[i, 'Laatste FTU'].strftime('%Y-%m-%d')

    return date_FTU0, date_FTU1


# Legacy
def get_data_meters(path_data):
    map_key = {
        'Data Oosterhout': 'Nijmegen Oosterhout',
        'Data Bottendaal': 'Nijmegen Bottendaal',
        'Data Oude stad': 'Bergen op Zoom oude stad',
        # 'Data Gouda Centrum': ' ',  # niet in FC?
        'Data Klarendal': 'Arnhem Klarendal',
        'Data Malburgen': 'Arnhem Malburgen',
        'Data Spijkerbuurt': 'Arnhem Spijkerbuurt',
        'Data Bergen op Zoom': 'Bergen op Zoom Oost',
        'Data Brielle': 'Brielle',
        'Data Morgenstond': 'Den Haag Morgenstond west',
        'Data Breda Brabantpark': 'Breda Brabantpark',
        'Data Gulden Bodem': 'Arnhem Gulden Bodem Schaarsbergen',
        'Data Biezen Wolfskuil': 'Nijmegen Biezen-Wolfskuil-Hatert ',
        'Data Spijkenisse': 'KPN Spijkernisse'
    }
    fn_teams = path_data + 'Data_20200101_extra/Weekrapportage FttX projecten - Week 22-2020.xlsx'
    xls = pd.ExcelFile(fn_teams)
    d_sheets_o = pd.read_excel(xls, None)
    d_sheets = {}
    for key_o in d_sheets_o:
        if key_o in map_key:
            d_sheets[map_key[key_o]] = d_sheets_o[key_o]

    return d_sheets


def get_data(subset, col, gpath_i, path_data, flag):
    if gpath_i is None:
        df_l = get_data_projects(subset, col)
    else:
        df_l = get_data_from_ingestbucket(gpath_i, col, path_data, subset, flag)
    return df_l


def get_start_time(df_l):
    # What does t_s stand for? Would prefer to use a descriptive variable name.
    t_s = {}
    for key in df_l:
        if df_l[key][~df_l[key].opleverdatum.isna()].empty:
            t_s[key] = pd.to_datetime(pd.Timestamp.now().strftime('%Y-%m-%d'))
        else:  # I'm not sure its desireable to hard-set dates like this. Might lead to unexpected behaviour.
            t_s[key] = pd.to_datetime(df_l[key]['opleverdatum']).min()
    return t_s


def get_timeline(t_s):
    x_axis = pd.date_range(min(t_s.values()), periods=1000 + 1, freq='D')
    return x_axis


def get_total_objects(df_l):  # Don't think this is necessary to calculate at this point, should be done later.
    total_objects = {k: len(v) for k, v in df_l.items()}
    # This hardcoded stuff can lead to unexpected behaviour. Should this still be in here?
    total_objects['Bergen op Zoom Noord  wijk 01 + Halsteren'] = 9.465  # not yet in FC, total from excel bouwstromen
    total_objects['Den Haag - Haagse Hout-Bezuidenhout West'] = 9.488  # not yet in FC, total from excel bouwstromen
    total_objects['Den Haag - Vrederust en Bouwlust'] = 11.918  # not yet in FC, total from excel bouwstromen
    return total_objects


# Function that adds columns to the source data, to be used in project specs
# hpend is a boolean column indicating whether an object has been delivered
# homes_completed is a boolean column indicating a home has been completed
# bis_gereed is a boolean column indicating whther the BIS for an object has been finished
def add_relevant_columns(df_l, year):
    for k, v in df_l.items():
        v['hpend'] = v.opleverdatum.apply(lambda x: object_is_hpend(x, '2020'))
        v['homes_completed'] = v.opleverstatus == '2'
        v['bis_gereed'] = v.opleverstatus != '0'
    return df_l


def object_is_hpend(opleverdatum, year):
    # Will return TypeError if opleverdatum is nan, in which case object is not hpend
    try:
        is_hpend = (opleverdatum >= year + '-01-01') & (opleverdatum <= year + '-12-31')
    except TypeError:
        is_hpend = False
    return is_hpend


# Calculates the amount of homes completed per project in a dictionary
def get_homes_completed(df_l):
    return {k: sum(v.homes_completed) for k, v in df_l.items()}


# Calculate the amount of objects per project that have been
# Permanently passed or completed
def get_HPend(df_l):
    return {k: sum(v.hpend) for k, v in df_l.items()}


# Objects that are ready for HAS
# These are obejcts for which:
# - a permission has been filled in (granted or rejected)
# - The BIS (basic infrastructure) is in place
def get_has_ready(df_l):
    return {k: len(v[~v.toestemming.isna() & v.bis_gereed]) for k, v in df_l.items()}


# Total ratio of completed objects v.s. completed + permantently passed objects.
def get_hc_hpend_ratio_total(hc, hpend):
    return round(sum(hc.values()) / sum(hpend.values()), 2)


# Calculates the ratio between homes completed v.s. completed + permantently passed objects per project
def get_hc_hpend_ratio(df_l):
    ratio_per_project = {}
    for project, data in df_l.items():
        try:
            ratio_per_project[project] = sum(data.homes_completed) / sum(data.hpend) * 100
        except ZeroDivisionError:
            # Dirty fix, check if it can be removed.
            ratio_per_project[project] = 0
    return ratio_per_project


def get_has_werkvoorraad(df_l):
    return sum(calculate_y_voorraad_act(df_l).values())


# Function to add relevant data to the source data_frames
# TODO: Convert dict of dataframes to single dataframe, and add this in further steps.
def preprocess_data(df_l, year):
    df_l = add_relevant_columns(df_l, year)
    return df_l


def calculate_projectspecs(df_l):
    homes_completed = get_homes_completed(df_l)
    homes_ended = get_HPend(df_l)
    has_ready = get_has_ready(df_l)
    hc_hpend_ratio = get_hc_hpend_ratio(df_l)
    hc_hp_end_ratio_total = get_hc_hpend_ratio_total(homes_completed, homes_ended)
    werkvoorraad = get_has_werkvoorraad(df_l)

    return hc_hp_end_ratio_total, hc_hpend_ratio, has_ready, homes_ended, werkvoorraad


def targets(x_prog, x_d, t_shift, date_FTU0, date_FTU1, rc1, d_real_l):
    # to add target info KPN in days uitgaande van FTU0 en FTU1
    y_target_l = {}
    t_diff = {}
    for key in t_shift:
        if (key in date_FTU0) & (key in date_FTU1):
            t_start = x_prog[x_d == date_FTU0[key]][0]
            t_max = x_prog[x_d == date_FTU1[key]][0]
            t_diff[key] = t_max - t_start - 14  # two weeks round up
            rc = 100 / t_diff[key]  # target naar KPN is 100% HPend
        if (key in date_FTU0) & (key not in date_FTU1):  # estimate target based on average projectspeed
            t_start = x_prog[x_d == date_FTU0[key]][0]
            t_diff[key] = (100 / (sum(rc1.values()) / len(rc1.values())) - 14)[0]  # two weeks round up
            rc = 100 / t_diff[key]  # target naar KPN is 100% HPend
        if (key not in date_FTU0):  # project has finished, estimate target on what has been done
            t_start = d_real_l[key].index.min()
            t_max = d_real_l[key].index.max()
            t_diff[key] = t_max - t_start - 14  # two weeks round up
            rc = 100 / t_diff[key]  # target naar KPN is 100% HPend

        b = -(rc * (t_start + 14))  # two weeks startup
        y_target = b + rc * x_prog
        y_target[y_target > 100] = 100
        y_target_l[key] = y_target

    for key in y_target_l:
        y_target_l[key][y_target_l[key] > 100] = 100
        y_target_l[key][y_target_l[key] < 0] = 0

    return y_target_l, t_diff


def prognose(df_l, t_s, x_d, tot_l, date_FTU0):
    x_prog = np.array(list(range(0, len(x_d))))
    cutoff = 85

    rc1 = {}
    rc2 = {}
    d_real_l = {}
    t_shift = {}
    y_prog_l = {}
    for key in df_l:  # to calculate prognoses for projects in FC
        d_real = df_l[key][~df_l[key]['opleverdatum'].isna()]
        if not d_real.empty:
            d_real = d_real.groupby(['opleverdatum']).agg({'sleutel': 'count'}).rename(columns={'sleutel': 'Aantal'})
            d_real.index = pd.to_datetime(d_real.index, format='%Y-%m-%d')
            d_real = d_real.sort_index()
            d_real = d_real[d_real.index < pd.Timestamp.now()]

            d_real = d_real.cumsum() / tot_l[key] * 100
            d_real[d_real.Aantal > 100] = 100  # only necessary for DH
            t_shift[key] = (d_real.index.min() - min(t_s.values())).days
            d_real.index = (d_real.index - d_real.index[0]).days + t_shift[key]
            d_real_l[key] = d_real

            d_rc1 = d_real[d_real.Aantal < cutoff]
            if len(d_rc1) > 1:
                rc1[key], b1 = np.polyfit(d_rc1.index, d_rc1, 1)
                y_prog1 = b1[0] + rc1[key][0] * x_prog
                y_prog_l[key] = y_prog1.copy()

            d_rc2 = d_real[d_real.Aantal >= cutoff]
            if (len(d_rc2) > 1) & (len(d_rc1) > 1):
                rc2[key], b2 = np.polyfit(d_rc2.index, d_rc2, 1)
                y_prog2 = b2[0] + rc2[key][0] * x_prog
                x_i, y_i = get_intersect([x_prog[0], y_prog1[0]], [x_prog[-1], y_prog1[-1]],
                                         [x_prog[0], y_prog2[0]], [x_prog[-1], y_prog2[-1]])
                y_prog_l[key][x_prog >= x_i] = y_prog2[x_prog >= x_i]
            # if (len(d_rc2) > 1) & (len(d_rc1) <= 1):
            #     rc1[key], b1 = np.polyfit(d_rc2.index, d_rc2, 1)
            #     y_prog1 = b1[0] + rc1[key][0] * x_prog
            #     y_prog_l[key] = y_prog1.copy()

    rc1_mean = sum(rc1.values()) / len(rc1.values())
    rc2_mean = sum(rc2.values()) / len(rc2.values())
    for key in df_l:
        if (key in rc1) & (key not in rc2):  # the case of 2 realisation dates, rc1 but no rc2
            if max(y_prog_l[key]) > cutoff:
                b2_mean = cutoff - (rc2_mean * x_prog[y_prog_l[key] >= cutoff][0])
                y_prog2 = b2_mean + rc2_mean * x_prog
                y_prog_l[key][y_prog_l[key] >= cutoff] = y_prog2[y_prog_l[key] >= cutoff]
        if (key in d_real_l) & (key not in y_prog_l):  # the case of only 1 realisation date
            b1_mean = -(rc1_mean * t_shift[key])
            y_prog1 = b1_mean + rc1_mean * x_prog
            b2_mean = cutoff - (rc2_mean * x_prog[y_prog1 >= cutoff][0])
            y_prog2 = b2_mean + rc2_mean * x_prog
            y_prog_l[key] = y_prog1.copy()
            y_prog_l[key][y_prog1 >= cutoff] = y_prog2[y_prog1 >= cutoff]
        if key not in d_real_l:  # the case of no realisation date
            t_shift[key] = x_prog[x_d == pd.Timestamp.now().strftime('%Y-%m-%d')][0]
            if key in date_FTU0:
                if not pd.isnull(date_FTU0[key]):
                    t_shift[key] = x_prog[x_d == date_FTU0[key]][0]
            b1_mean = -(rc1_mean * (t_shift[key] + 14))  # to include delay of two week
            y_prog1 = b1_mean + rc1_mean * x_prog
            b2_mean = cutoff - (rc2_mean * x_prog[y_prog1 >= cutoff][0])
            y_prog2 = b2_mean + rc2_mean * x_prog
            y_prog_l[key] = y_prog1.copy()
            y_prog_l[key][y_prog1 >= cutoff] = y_prog2[y_prog1 >= cutoff]

    for key in y_prog_l:
        y_prog_l[key][y_prog_l[key] > 100] = 100
        y_prog_l[key][y_prog_l[key] < 0] = 0

    return rc1, rc2, d_real_l, y_prog_l, x_prog, t_shift, cutoff


def overview(x_d, y_prog_l, tot_l, d_real_l, HP, y_target_l):

    df_prog = pd.DataFrame(index=x_d, columns=['d'], data=0)
    for key in y_prog_l:
        y_prog = y_prog_l[key] / 100 * tot_l[key]
        df_prog += pd.DataFrame(index=x_d, columns=['d'], data=y_prog).diff().fillna(0)

    df_target = pd.DataFrame(index=x_d, columns=['d'], data=0)
    for key in y_target_l:
        y_target = y_target_l[key] / 100 * tot_l[key]
        df_target += pd.DataFrame(index=x_d, columns=['d'], data=y_target).diff().fillna(0)

    df_real = pd.DataFrame(index=x_d, columns=['d'], data=0)
    for key in d_real_l:
        y_real = (d_real_l[key] / 100 * tot_l[key]).diff().fillna((d_real_l[key] / 100 * tot_l[key]).iloc[0])
        y_real = y_real.rename(columns={'Aantal': 'd'})
        y_real.index = x_d[y_real.index]
        df_real = df_real.add(y_real, fill_value=0)

    df_plan = pd.DataFrame(index=x_d, columns=['d'], data=0)
    y_plan = pd.DataFrame(index=pd.date_range(start='30-12-2019', periods=len(HP['HPendT']), freq='W-MON'),
                          columns=['d'], data=HP['HPendT'])
    y_plan = y_plan.cumsum().resample('D').mean().interpolate().diff().fillna(y_plan.iloc[0])
    df_plan = df_plan.add(y_plan, fill_value=0)

    # plot option
    # import matplotlib.pyplot as plt
    # test = df_real.resample('M', closed='left', loffset=None).sum()['d']
    # fig, ax = plt.subplots(figsize=(14,8))
    # ax.bar(x=test.index[0:15].strftime('%Y-%m'), height=test[0:15], width=0.5)
    # plt.savefig('Graphs/jaaroverzicht_2019_2020.png')

    return df_prog, df_target, df_real, df_plan


def graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad, res):
    if 'W' in res:
        n_now = int((pd.Timestamp.now() - pd.to_datetime('2019-12-30')).days / 7) + 1
        n_d = int((pd.Timestamp.now() - pd.to_datetime('2020-' + str(datetime.date.today().month) + '-01')).days / 7)
        x_ticks = list(range(n_now - n_d, n_now + 5 - n_d))
        x_ticks_text = [datetime.datetime.strptime('2020-W' + str(int(el-1)) + '-1', "%Y-W%W-%w").date().strftime(
            '%Y-%m-%d') + '<br>W' + str(el) for el in x_ticks]
        x_range = [n_now - n_d - 0.5, n_now + 4.5 - n_d]
        y_range = [0, 3000]
        width = 0.08
        text_title = 'Maandoverzicht'
        period = ['2019-12-23', '2020-12-27']
        close = 'left'
        loff = '-1W-MON'
        x = df_prog[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum().index.week.to_list()
        x[0] = 0
    if 'M' == res:
        n_now = datetime.date.today().month
        x_ticks = list(range(0, 13))
        x_ticks_text = ['dec', 'jan', 'feb', 'maa', 'apr', 'mei', 'jun', 'jul', 'aug', 'sep', 'okt', 'nov', 'dec']
        x_range = [0.5, 12.5]
        y_range = [0, 18000]
        width = 0.2
        text_title = 'Jaaroverzicht'
        period = ['2019-12-23', '2020-12-27']
        close = 'left'
        loff = None
        x = df_prog[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum().index.month.to_list()
        x[0] = 0

    prog = df_prog[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d'].to_list()
    target = df_target[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d'].to_list()
    real = df_real[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d'].to_list()
    plan = df_plan[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d'].to_list()
    plan[0:n_now] = real[0:n_now]  # gelijk trekken afgelopen periode

    if 'M' == res:
        jaaroverzicht = dict(id='jaaroverzicht', target=str(round(sum(target[1:]))), real=str(round(sum(real[1:]))),
                             plan=str(round(sum(plan[n_now:]) - real[n_now])), prog=str(round(sum(prog[n_now:]) - real[n_now])),
                             HC_HPend=str(HC_HPend), HAS_werkvoorraad=str(HAS_werkvoorraad), prog_c='pretty_container')
        if jaaroverzicht['prog'] < jaaroverzicht['plan']:
            jaaroverzicht['prog_c'] = 'pretty_container_red'

    bar_now = dict(x=[n_now],
                   y=[y_range[1]],
                   name='Huidige week',
                   type='bar',
                   marker=dict(color='rgb(0, 0, 0)'),
                   width=0.5*width,
                   )
    bar_t = dict(x=[el - 0.5*width for el in x],
                 y=target,
                 name='Outlook (KPN)',
                 type='bar',
                 marker=dict(color='rgb(170, 170, 170)'),
                 width=width,
                 )
    bar_pr = dict(x=x,
                  y=prog,
                  name='Voorspelling (VQD)',
                  mode='markers',
                  marker=dict(color='rgb(200, 200, 0)', symbol='diamond', size=15),
                  #   width=0.2,
                  )
    bar_r = dict(x=[el + 0.5*width for el in x],
                 y=real,
                 name='Realisatie (FC)',
                 type='bar',
                 marker=dict(color='rgb(0, 200, 0)'),
                 width=width,
                 )
    bar_pl = dict(x=x,
                  y=plan,
                  name='Planning HP (VWT)',
                  type='lines',
                  marker=dict(color='rgb(200, 0, 0)'),
                  width=width,
                  )
    fig = {
           'data': [bar_pr, bar_pl, bar_r, bar_t, bar_now],
           'layout': {
                      'barmode': 'stack',
                      #   'clickmode': 'event+select',
                      'showlegend': True,
                      'legend': {'orientation': 'h', 'x': -0.075, 'xanchor': 'left', 'y': -0.25, 'font': {'size': 10}},
                      'height': 300,
                      'margin': {'l': 5, 'r': 15, 'b': 5, 't': 40},
                      'title': {'text': text_title},
                      'xaxis': {'range': x_range,
                                'tickvals': x_ticks,
                                'ticktext': x_ticks_text,
                                'title': ' '},
                      'yaxis': {'range': y_range, 'title': 'Aantal HPend'},
                      #   'annotations': [dict(x=x_ann, y=y_ann, text=jaaroverzicht, xref="x", yref="y",
                      #                   ax=0, ay=0, alignment='left', font=dict(color="black", size=15))]
                      },
          }
    if 'W' in res:
        record = dict(id='graph_targets_W', figure=fig)
        return record
    if 'M' == res:
        record = dict(id='graph_targets_M', figure=fig)
        return record, jaaroverzicht


def meters(d_sheets, tot_l, x_d, y_target_l):
    teams_BIS_all = []
    m_BIS_all = []
    teams_HAS_all = []
    m_HAS_all = []
    w_BIS_all = []
    w_HAS_all = []
    y_BIS = {}
    y_schouw = {}
    y_HASm = {}
    y_HAS = {}
    y_target = {}

    for key in d_sheets:
        m_BIST = d_sheets[key].iloc[10, 1]
        if np.isnan(m_BIST):
            m_BIST = tot_l[key] * 7  # based on average
        m_BIS = d_sheets[key].iloc[10, 2:].fillna(0)  # data from week 1 2020
        teams_BIS = d_sheets[key].iloc[5, 2:].fillna(0)
        w_BIS = d_sheets[key].iloc[12, 2:].fillna(0)
        teams_BIS_all += teams_BIS.to_list()
        m_BIS_all += m_BIS.to_list()
        w_BIS_all += w_BIS.to_list()

        m_HAST = d_sheets[key].iloc[11, 1]
        if np.isnan(m_HAST):
            m_HAST = tot_l[key] * 3  # based on average
        m_HAS = d_sheets[key].iloc[11, 2:].fillna(0)
        teams_HAS = d_sheets[key].iloc[7, 2:].fillna(0)
        w_HP = d_sheets[key].iloc[28, 2:].fillna(0)
        w_2 = d_sheets[key].iloc[24, 2:].fillna(0)
        # w_33 = d_sheets[key].iloc[27, 2:].fillna(0)
        # w_35 = d_sheets[key].iloc[26, 2:].fillna(0)
        # w_31 = d_sheets[key].iloc[25, 2:].fillna(0)
        # w_11 = d_sheets[key].iloc[23, 2:].fillna(0)
        # w_1 = d_sheets[key].iloc[22, 2:].fillna(0)
        # w_5 = d_sheets[key].iloc[21, 2:].fillna(0)
        teams_HAS_all += teams_HAS.to_list()
        m_HAS_all += m_HAS.to_list()
        w_HAS_all += (w_HP + w_2).to_list()

        w_SLB = d_sheets[key].iloc[32, 2:].fillna(0)
        w_SHB = d_sheets[key].iloc[36, 2:].fillna(0)

        if key in y_target_l:
            y_target_p = pd.DataFrame(index=x_d, columns=['y_target'], data=y_target_l[key])
        else:
            y_target_p = pd.DataFrame(index=x_d, columns=['y_target'], data=0)
        x_target = y_target_p['2019-12-30':'2020-12-21'].resample(
            'W-MON', closed='right', loffset='-1W-MON').mean().index.strftime('%Y-%m-%d').to_list()
        y_target[key] = y_target_p['2019-12-30':'2020-12-21'].resample(
            'W-MON', closed='right', loffset='-1W-MON').mean()['y_target'].to_list()

        y_BIS[key] = (m_BIS.cumsum() / m_BIST * 100).to_list() + [0] * (len(y_target[key]) - len(m_BIS))
        y_HASm[key] = (m_HAS.cumsum() / m_HAST * 100).to_list() + [0] * (len(y_target[key]) - len(m_HAS))
        y_HAS[key] = ((w_HP + w_2).cumsum() / tot_l[key] * 100).to_list() + [0] * (len(y_target[key]) - len(w_HP))
        y_schouw[key] = ((w_SLB + w_SHB).cumsum() / tot_l[key] * 100).to_list() + [0] * (len(y_target[key]) - len(w_SLB))

    m_gegraven = [el1 + el2 for el1, el2 in zip(m_BIS_all, m_HAS_all)]
    rc_BIS, _ = np.polyfit(teams_BIS_all, m_gegraven, 1)  # aantal meters BIS per team per week
    rc_HAS, _ = np.polyfit(teams_HAS_all, w_HAS_all, 1)
    w_now = int((pd.Timestamp.now() - pd.to_datetime('2019-12-30')).days / 7) + 1
    advies = {}
    for key in y_target:
        BIS_advies = round((y_target[key][w_now] - y_BIS[key][w_now]) / 100 * tot_l[key] * 7 / rc_BIS)  # gem 7m BIS per woning
        if BIS_advies <= 0:
            BIS_advies = 'On target!'
        HAS_advies = round((y_target[key][w_now] - y_HAS[key][w_now]) / 100 * tot_l[key] / rc_HAS)
        if HAS_advies <= 0:
            HAS_advies = 'On target!'
        advies[key] = 'Advies:<br>' + 'BIS teams: ' + str(BIS_advies) + '<br>HAS teams: ' + str(HAS_advies)

    return x_target, y_target, y_BIS, y_HASm, y_HAS, y_schouw, advies


def meters_graph(x_target, y_target, y_prog_l, y_BIS, y_HASm, y_HAS, y_schouw, advies):
    fig = {}
    for key in y_prog_l:
        if key in y_target:
            fig = {'data': [{
                            'x': x_target,
                            'y': y_schouw[key],
                            'mode': 'markers',
                            'marker': {'symbol': 'triangle-down', 'color': 'rgb(0, 200, 0)'},
                            'name': 'Geschouwd',
                            },
                            {
                            'x': x_target,
                            'y': y_BIS[key],
                            'mode': 'markers',
                            'marker': {'symbol': 'x', 'color': 'rgb(0, 200, 0)'},
                            'name': 'BIS-gegraven',
                            },
                            {
                            'x': x_target,
                            'y': y_HASm[key],
                            'mode': 'markers',
                            'marker': {'symbol': 'circle', 'color': 'rgb(0, 200, 0)'},
                            'name': 'HAS-gegraven',
                            },
                            {
                            'x': x_target,
                            'y': y_HAS[key],
                            'mode': 'markers',
                            'marker': {'symbol': 'diamond', 'color': 'rgb(0, 200, 0)'},
                            'name': 'HAS-opgeleverd',
                            },
                            {
                            'x': x_target,
                            'y': y_target[key],
                            'mode': 'line',
                            'line': dict(color='rgb(170, 170, 170)'),
                            'name': 'Outlook (KPN)',
                            }],
                   'layout': {
                              'xaxis': {'title': 'Opleverdatum [d]', 'range': ['2019-12-30', '2020-12-28']},
                              'yaxis': {'title': 'Fase afgerond [%]', 'range': [0, 110]},
                              'title': {'text': 'Voortgang fase vs outlook KPN:'},
                              'showlegend': True,
                              'legend': {'x': 1.2, 'xanchor': 'right', 'y': 1},
                              'height': 350,
                              'annotations': [dict(x='2020-10-12', y=50, text=advies[key], xref="x", yref="y",
                                                   ax=0, ay=0, alignment='left')]
                              }
                   }
        else:
            fig = {'layout': {
                              'xaxis': {'title': 'Opleverdatum [d]', 'range': ['2019-12-30', '2020-12-28']},
                              'yaxis': {'title': 'Fase afgerond [%]', 'range': [0, 110]},
                              'title': {'text': 'Voortgang fase vs outlook KPN:'},
                              'showlegend': True,
                              'legend': {'x': 1.2, 'xanchor': 'right', 'y': 1},
                              'height': 350,
                              }
                   }

        record = dict(id=key, figure=fig)
        firestore.Client().collection('Graphs').document(record['id']).set(record)


def prognose_graph(x_d, y_prog_l, d_real_l, y_target_l):
    record_dict = {}
    for key in y_prog_l:
        fig = {'data': [{
                         'x': list(x_d.strftime('%Y-%m-%d')),
                         'y': list(y_prog_l[key]),
                         'mode': 'lines',
                         'line': dict(color='rgb(200, 200, 0)'),
                         'name': 'Voorspelling (VQD)',
                         }],
               'layout': {
                          'xaxis': {'title': 'Opleverdatum [d]', 'range': ['2020-01-01', '2020-12-31']},
                          'yaxis': {'title': 'Opgeleverd HPend [%]', 'range': [0, 110]},
                          'title': {'text': 'Voortgang project vs outlook KPN:'},
                          'showlegend': True,
                          'legend': {'x': 1.2, 'xanchor': 'right', 'y': 1},
                          'height': 350
                           },
               }
        if key in d_real_l:
            fig['data'] = fig['data'] + [{
                                          'x': list(x_d[d_real_l[key].index.to_list()].strftime('%Y-%m-%d')),
                                          'y': d_real_l[key]['Aantal'].to_list(),
                                          'mode': 'markers',
                                          'line': dict(color='rgb(0, 200, 0)'),
                                          'name': 'Realisatie (FC)',
                                          }]

        if key in y_target_l:
            fig['data'] = fig['data'] + [{
                                          'x': list(x_d.strftime('%Y-%m-%d')),
                                          'y': list(y_target_l[key]),
                                          'mode': 'lines',
                                          'line': dict(color='rgb(170, 170, 170)'),
                                          'name': 'Outlook (KPN)',
                                          }]
        record = dict(id='project_' + key, figure=fig)
        record_dict[key] = record
    return record_dict


def map_redenen():
    reden_l = dict(
                    R0='Geplande aansluiting',
                    R00='Geplande aansluiting',
                    R1='Geen toestemming bewoner',
                    R01='Geen toestemming bewoner',
                    R2='Geen toestemming VVE / WOCO',
                    R02='Geen toestemming VVE / WOCO',
                    R3='Bewoner na 3 pogingen niet thuis',
                    R4='Nieuwbouw (woning nog niet gereed)',
                    R5='Hoogbouw obstructie (blokkeert andere bewoners)',
                    R6='Hoogbouw obstructie (wordt geblokkeerd door andere bewoners)',
                    R7='Technische obstructie',
                    R8='Meterkast voldoet niet aan eisen',
                    R9='Pand staat leeg',
                    R10='Geen graafvergunning',
                    R11='Aansluitkosten boven normbedrag niet gedekt',
                    R12='Buiten het uitrolgebied',
                    R13='Glasnetwerk van een andere operator',
                    R14='Geen vezelcapaciteit',
                    R15='Geen woning',
                    R16='Sloopwoning (niet voorbereid)',
                    R17='Complex met 1 aansluiting op ander adres',
                    R18='Klant niet bereikbaar',
                    R19='Bewoner niet thuis, wordt opnieuw ingepland',
                    R20='Uitrol na vraagbundeling, klant neemt geen dienst',
                    R21='Wordt niet binnen dit project aangesloten',
                    R22='Vorst, niet planbaar',
                    R_geen='Geen reden'
    )
    record = dict(id='reden_mapping', map=reden_l)
    firestore.Client().collection('Graphs').document(record['id']).set(record)


def masks_phases(pkey, df_l):
    df = df_l[pkey]
    batch = firestore.Client().batch()
    bar_m = {}
    bar_m['SchouwenLB0-mask'] = (df['toestemming'].isna()) & \
                                (df['soort_bouw'] == 'Laag')
    bar_m['SchouwenLB1-mask'] = (~df['toestemming'].isna()) & \
                                (df['soort_bouw'] == 'Laag')
    bar_m['SchouwenHB0-mask'] = (df['toestemming'].isna()) & \
                                (df['soort_bouw'] != 'Laag')
    bar_m['SchouwenHB1-mask'] = (~df['toestemming'].isna()) &\
                                (df['soort_bouw'] != 'Laag')
    bar_m['BISLB0-mask'] = (df['opleverstatus'] == '0') & \
                           (df['soort_bouw'] == 'Laag')
    bar_m['BISLB1-mask'] = (df['opleverstatus'] != '0') & \
                           (df['soort_bouw'] == 'Laag')
    bar_m['BISHB0-mask'] = (df['opleverstatus'] == '0') & \
                           (df['soort_bouw'] != 'Laag')
    bar_m['BISHB1-mask'] = (df['opleverstatus'] != '0') & \
                           (df['soort_bouw'] != 'Laag')
    bar_m['Montage-lasDPLB0-mask'] = (df['laswerkdpgereed'] == '0') & \
                                     (df['soort_bouw'] == 'Laag')
    bar_m['Montage-lasDPLB1-mask'] = (df['laswerkdpgereed'] == '1') & \
                                     (df['soort_bouw'] == 'Laag')
    bar_m['Montage-lasDPHB0-mask'] = (df['laswerkdpgereed'] == '0') & \
                                     (df['soort_bouw'] != 'Laag')
    bar_m['Montage-lasDPHB1-mask'] = (df['laswerkdpgereed'] == '1') & \
                                     (df['soort_bouw'] != 'Laag')
    bar_m['Montage-lasAPLB0-mask'] = (df['laswerkapgereed'] == '0') & \
                                     (df['soort_bouw'] == 'Laag')
    bar_m['Montage-lasAPLB1-mask'] = (df['laswerkapgereed'] == '1') & \
                                     (df['soort_bouw'] == 'Laag')
    bar_m['Montage-lasAPHB0-mask'] = (df['laswerkapgereed'] == '0') & \
                                     (df['soort_bouw'] != 'Laag')
    bar_m['Montage-lasAPHB1-mask'] = (df['laswerkapgereed'] == '1') & \
                                     (df['soort_bouw'] != 'Laag')
    bar_m['HASLB0-mask'] = (df['opleverdatum'].isna()) & \
                           (df['soort_bouw'] == 'Laag')
    bar_m['HASLB1-mask'] = (df['opleverstatus'] == '2') & \
                           (df['soort_bouw'] == 'Laag')
    bar_m['HASLB1HP-mask'] = (df['opleverstatus'] != '2') & \
                             (~df['opleverdatum'].isna()) & \
                             (df['soort_bouw'] == 'Laag')
    bar_m['HASHB0-mask'] = (df['opleverdatum'].isna()) & \
                           (df['soort_bouw'] != 'Laag')
    bar_m['HASHB1-mask'] = (df['opleverstatus'] == '2') & \
                           (df['soort_bouw'] != 'Laag')
    bar_m['HASHB1HP-mask'] = (df['opleverstatus'] != '2') & \
                             (~df['opleverdatum'].isna()) & \
                             (df['soort_bouw'] != 'Laag')

    bar = {}
    bar_names = []
    mask = True
    # begin state:
    for key2 in bar_m:
        len_b = (bar_m[key2] & mask).value_counts()
        if True in len_b:
            bar[key2[0:-5]] = str(len_b[True])
        else:
            bar[key2[0:-5]] = str(0)
    record = dict(id=pkey + '_bar_filters_0', bar=bar)
    bar_names += '0'
    batch.set(firestore.Client().collection('Graphs').document(record['id']), record)
    # after one click:
    for key2 in bar_m:
        mask = bar_m[key2]
        bar = {}
        for key3 in bar_m:
            len_b = (bar_m[key3] & mask).value_counts()
            if True in len_b:
                bar[key3[0:-5]] = str(len_b[True])
            else:
                bar[key3[0:-5]] = str(0)
        record = dict(id=pkey + '_bar_filters_0' + key2[0:-5], bar=bar, mask=json.dumps(df[mask].sleutel.to_list()))
        bar_names += ['0' + key2[0:-5]]
        batch.set(firestore.Client().collection('Graphs').document(record['id']), record)
    batch.commit()
    batch = firestore.Client().batch()
    # print('23')
    # after second click:
    ii = 0
    for key2 in bar_m:
        mask = bar_m[key2]
        for key3 in bar_m:
            mask2 = bar_m[key3]
            bar = {}
            for key4 in bar_m:
                len_b = (bar_m[key4] & mask & mask2).value_counts()
                if True in len_b:
                    bar[key4[0:-5]] = str(len_b[True])
                else:
                    bar[key4[0:-5]] = str(0)
            record = dict(id=pkey + '_bar_filters_0' + key2[0:-5] + key3[0:-5],
                          bar=bar,
                          mask=json.dumps(df[mask & mask2].sleutel.to_list()))
            bar_names += ['0' + key2[0:-5] + key3[0:-5]]
            batch.set(firestore.Client().collection('Graphs').document(record['id']), record)
            ii += 1
            if (ii % 150 == 0):
                # print(ii)
                batch.commit()
                batch = firestore.Client().batch()
    batch.commit()

    return bar_m


def set_bar_names(bar_m):
    bar_names = ['0']
    for key2 in bar_m:
        bar_names += ['0' + key2[0:-5]]
    for key2 in bar_m:
        for key3 in bar_m:
            bar_names += ['0' + key2[0:-5] + key3[0:-5]]
    record = dict(id='bar_names', bar_names=bar_names)
    firestore.Client().collection('Graphs').document(record['id']).set(record)


def consume(df_l):
    t = time.time()
    for key in df_l:  # niet nodig in gcp
        df = df_l[key]  # niet nodig in gcp
        records = df.to_dict('records')  # niet nodig in gcp
        batch = firestore.Client().batch()
        for i, row in enumerate(records):
            record = row
            batch.set(firestore.Client().collection('Projects').document(record['sleutel']), record)
            if (i + 1) % 500 == 0:
                batch.commit()
        batch.commit()
        print(key + ' ' + str(i+1))
        print('Time: ' + str((time.time() - t)/60) + ' minutes')


def get_data_projects(subset, col):
    t = time.time()
    df_l = {}
    for key in subset:
        docs = firestore.Client().collection('Projects').where('project', '==', key).stream()
        records = []
        for doc in docs:
            records += [doc.to_dict()]
        if records != []:
            df_l[key] = pd.DataFrame(records)[col].fillna(np.nan)
        else:
            df_l[key] = pd.DataFrame(columns=col).fillna(np.nan)
        # to correct for datetime value at HUB
        df_l[key].loc[~df_l[key]['opleverdatum'].isna(), ('opleverdatum')] = \
            [el[0:10] for el in df_l[key][~df_l[key]['opleverdatum'].isna()]['opleverdatum']]
        df_l[key].loc[~df_l[key]['hasdatum'].isna(), ('hasdatum')] = \
            [el[0:10] for el in df_l[key][~df_l[key]['hasdatum'].isna()]['hasdatum']]

        print(key)
        print('Time: ' + str((time.time() - t)/60) + ' minutes')

    return df_l


def speed_graph(df_l, tot_l, rc1, rc2, cutoff):
    perc_complete = {}
    rc = {}
    for key in df_l:
        af = len(df_l[key][~df_l[key]['opleverdatum'].isna()])
        if tot_l[key] != 0:
            perc = int(af / tot_l[key] * 100)
        else:
            perc = 0
        if perc != 100:
            perc_complete[key] = perc
            if key in rc2:
                rc[key] = rc2[key][0] / 100 * tot_l[key]
            elif key in rc1:
                rc[key] = rc1[key][0] / 100 * tot_l[key]
            else:
                rc[key] = 0

    rc1_mean_t = []
    rc2_mean_t = []
    for key in rc:
        if (perc_complete[key] > 5) & (perc_complete[key] < cutoff):
            rc1_mean_t += [rc[key]]
        if (perc_complete[key] >= cutoff) & (perc_complete[key] < 100):
            rc2_mean_t += [rc[key]]
    rc1_mean_t = sum(rc1_mean_t) / len(rc1_mean_t)
    rc2_mean_t = sum(rc2_mean_t) / len(rc2_mean_t)

    fig = {'data': [{
                     'x': [5, cutoff, cutoff, 100, 100, cutoff, cutoff, 5],
                     'y': [rc1_mean_t*0, rc1_mean_t*0,
                           rc2_mean_t*0, rc2_mean_t*0,
                           rc2_mean_t, rc2_mean_t,
                           rc1_mean_t, rc1_mean_t
                           ],
                     'name': 'Trace 2',
                     'mode': 'lines',
                     'fill': 'toself',
                     'opacity': 1,
                     'line': {'color': 'rgb(200, 0, 0)'}
                     },
                    {
                     'x': list(perc_complete.values()),
                     'y': list(rc.values()),
                     'text': list(perc_complete.keys()),
                     'name': 'Trace 1',
                     'mode': 'markers',
                     'marker': {'size': 15, 'color': 'rgb(200, 200, 0)'}
                     }],
           'layout': {'clickmode': 'event+select',
                      'xaxis': {'title': 'Opgeleverd (HP & HC) [%]', 'range': [-2.5, 100]},
                      'yaxis': {'title': 'Gemiddelde opleversnelheid [w/d]', 'range': [-5, 30]},
                      'showlegend': False,
                      'title': {'text': 'Een punt ofwel project binnen het rode vlak levert te langzaam op,' +
                                        ' klik erop voor meer informatie!'},
                      'height': 300,
                      'margin': {'l': 5, 'r': 15, 'b': 5, 't': 40},
                      }
           }
    record = dict(id='project_performance', figure=fig)
    firestore.Client().collection('Graphs').document(record['id']).set(record)


def performance_matrix(x_d, y_target_l, d_real_l, tot_l, t_diff, y_voorraad_act):
    n_now = int((pd.Timestamp.now() - x_d[0]).days)
    x = []
    y = []
    names = []
    for key in y_target_l:
        if key in d_real_l:
            x += [round((d_real_l[key].max() - y_target_l[key][n_now]))[0]]
        else:
            x += [0]
        y_voorraad = tot_l[key] / t_diff[key] * 7 * 9  # op basis van 9 weken voorraad
        if y_voorraad > 0:
            y += [round(y_voorraad_act[key] / y_voorraad * 100)]
        else:
            y += [0]
        names += [key]

    x_max = 30  # + max([abs(min(x)), abs(max(x))])
    x_min = - x_max
    y_min = - 30
    y_max = 250  # + max([abs(min(y)), abs(max(y))])
    y_voorraad_p = 90
    fig = {'data': [
                    {
                     'x': [x_min, 1 / 70 * x_min, 1 / 70 * x_min, x_min],
                     'y': [y_min, y_min, y_voorraad_p, y_voorraad_p],
                     'name': 'Trace 2',
                     'mode': 'lines',
                     'fill': 'toself',
                     'opacity': 1,
                     'line': {'color': 'rgb(200, 0, 0)'}
                     },
                    {
                     'x': [1 / 70 * x_min, 1 / 70 * x_max, 1 / 70 * x_max, 15, 15, 1 / 70 * x_min],
                     'y': [y_min, y_min, y_voorraad_p, y_voorraad_p, 150, 150],
                     'name': 'Trace 2',
                     'mode': 'lines',
                     'fill': 'toself',
                     'opacity': 1,
                     'line': {'color': 'rgb(0, 200, 0)'}
                     },
                    {
                     'x': [x_min, 1 / 70 * x_min, 1 / 70 * x_min, 15,  15, 1 / 70 * x_max,
                           1 / 70 * x_max,  x_max, x_max, x_min, x_min, 1 / 70 * x_min],
                     'y': [y_voorraad_p, y_voorraad_p, 150, 150, y_voorraad_p, y_voorraad_p,
                           y_min, y_min, y_max, y_max, y_voorraad_p, y_voorraad_p],
                     'name': 'Trace 2',
                     'mode': 'lines',
                     'fill': 'toself',
                     'opacity': 1,
                     'line': {'color': 'rgb(200, 200, 0)'}
                     },
                    {
                     'x':  x,
                     'y': y,
                     'text': names,
                     'name': 'Trace 1',
                     'mode': 'markers',
                     'marker': {'size': 15, 'color': 'rgb(0, 0, 0)'}
                     }],
           'layout': {'clickmode': 'event+select',
                      'xaxis': {'title': 'Procent voor of achter HPEnd op KPNTarget', 'range': [x_min, x_max],
                                'zeroline': False},
                      'yaxis': {'title': 'Procent voor of achter op verwachte werkvoorraad', 'range': [y_min, y_max], 'zeroline': False},
                      'showlegend': False,
                      'title': {'text': 'Krijg alle projecten in het groene vlak door de pijlen te volgen'},
                      'annotations': [dict(x=-20, y=50, ax=0, ay=40, xref="x", yref="y",
                                           text='Verhoog schouw of BIS capaciteit', alignment='left',
                                           showarrow=True, arrowhead=2)] +
                                     [dict(x=20, y=50, ax=0, ay=40, xref="x", yref="y",
                                           text='Verhoog schouw of BIS capaciteit', alignment='left',
                                           showarrow=True, arrowhead=2)] +
                                     [dict(x=-23.5, y=135, ax=-100, ay=0, xref="x", yref="y",
                                           text='Verhoog HAS capaciteit',
                                           alignment='left', showarrow=True, arrowhead=2)] +
                                     [dict(x=-23.5, y=65, ax=-100, ay=0, xref="x", yref="y",
                                           text='Verruim afspraak KPN',
                                           alignment='left', showarrow=True, arrowhead=2)] +
                                     [dict(x=23.5, y=135, ax=100, ay=0, xref="x", yref="y",
                                           text='Verlaag HAS capcaciteit',
                                           alignment='right', showarrow=True, arrowhead=2)] +
                                     [dict(x=23.5, y=65, ax=100, ay=0, xref="x", yref="y",
                                           text='Verscherp afspraak KPN',
                                           alignment='right', showarrow=True, arrowhead=2)] +
                                     [dict(x=20, y=160, ax=0, ay=-40, xref="x", yref="y",
                                           text='Verlaag schouw of BIS capaciteit', alignment='left',
                                           showarrow=True, arrowhead=2)] +
                                     [dict(x=-20, y=160, ax=0, ay=-40, xref="x", yref="y",
                                           text='Verlaag schouw of BIS capaciteit', alignment='left',
                                           showarrow=True, arrowhead=2)],
                      'height': 500,
                      'width': 1700,
                      'margin': {'l': 60, 'r': 15, 'b': 40, 't': 40},
                      }
           }
    record = dict(id='project_performance', figure=fig)
    return record


def set_filters(df_l):
    filters = []
    for key in df_l:
        filters += [{'label': key, 'value': key}]
    record = dict(id='pnames', filters=filters)
    firestore.Client().collection('Graphs').document(record['id']).set(record)


def get_intersect(a1, a2, b1, b2):
    """
    Returns the point of intersection of the lines passing through a2,a1 and b2,b1.
    a1: [x, y] a point on the first line
    a2: [x, y] another point on the first line
    b1: [x, y] a point on the second line
    b2: [x, y] another point on the second line
    """
    s = np.vstack([a1, a2, b1, b2])      # s for stacked
    h = np.hstack((s, np.ones((4, 1))))  # h for homogeneous
    l1 = np.cross(h[0], h[1])           # get first line
    l2 = np.cross(h[2], h[3])           # get second line
    x, y, z = np.cross(l1, l2)          # point of intersection
    if z == 0:                          # lines are parallel
        return (float('inf'), float('inf'))
    return (x/z, y/z)


def info_table(tot_l, d_real_l, HP, y_target_l, x_d, HC_HPend_l, Schouw_BIS, HPend_l, n_err):
    n_w = int((pd.Timestamp.now() - pd.to_datetime('2019-12-30')).days / 7) + 1
    n_d = int((pd.Timestamp.now() - x_d[0]).days)
    n_dw = int((pd.to_datetime('2019-12-30') - x_d[0]).days) + (n_w - 1) * 7
    col = ['project', 'KPN HPend - W' + str(n_w - 1), 'Real HPend - W' + str(n_w - 1), 'Diff - W' + str(n_w - 1),
           'KPN HPend - W' + str(n_w), 'Real HPend - W' + str(n_w),  'Diff - W' + str(n_w), 'HC / HP actueel', 'Errors FC - BC']
    records = []
    for key in d_real_l:
        if d_real_l[key].max()[0] < 100:
            record = dict(project=key)
            record[col[1]] = round(y_target_l[key][n_dw - 7] / 100 * tot_l[key])
            real_latest = d_real_l[key][d_real_l[key].index <= n_dw - 7]
            if not real_latest.empty:
                record[col[2]] = round(real_latest.iloc[-1][0] / 100 * tot_l[key])
            else:
                record[col[2]] = 0
            record[col[3]] = record[col[2]] - record[col[1]]
            record[col[4]] = round(y_target_l[key][n_d] / 100 * tot_l[key])
            real_latest = d_real_l[key][d_real_l[key].index <= n_d]
            if not real_latest.empty:
                record[col[5]] = round(real_latest.iloc[-1][0] / 100 * tot_l[key])
            else:
                record[col[5]] = 0
            record[col[6]] = record[col[5]] - record[col[4]]
            record[col[7]] = round(HC_HPend_l[key])
            record[col[8]] = n_err[key]
            records += [record]
    df_table = pd.DataFrame(records).to_json(orient='records')
    record = dict(id='info_table', table=df_table, col=col)
    return record


def update_y_prog_l(date_FTU0, d_real_l, t_shift, rc1, rc2, y_prog_l, x_d, x_prog, cutoff):
    rc1_mean = sum(rc1.values()) / len(rc1.values())
    rc2_mean = sum(rc2.values()) / len(rc2.values())
    for key in date_FTU0:
        if key not in d_real_l:  # the case of no realisation date
            t_shift[key] = x_prog[x_d == date_FTU0[key]][0]
            b1_mean = -(rc1_mean * (t_shift[key] + 14))  # to include delay of two week
            y_prog1 = b1_mean + rc1_mean * x_prog
            b2_mean = cutoff - (rc2_mean * x_prog[y_prog1 >= cutoff][0])
            y_prog2 = b2_mean + rc2_mean * x_prog
            y_prog_l[key] = y_prog1.copy()
            y_prog_l[key][y_prog1 >= cutoff] = y_prog2[y_prog1 >= cutoff]
            y_prog_l[key][y_prog_l[key] > 100] = 100
            y_prog_l[key][y_prog_l[key] < 0] = 0

    return y_prog_l, t_shift


def calculate_y_voorraad_act(df_l):
    y_voorraad_act = {}
    for key in df_l:
        y_voorraad_act[key] = len(df_l[key][(~df_l[key].toestemming.isna()) &
                                            (df_l[key].opleverstatus != '0') &
                                            (df_l[key].opleverdatum.isna())])

    return y_voorraad_act


def empty_collection(subset):
    t_start = time.time()
    for key in subset:
        i = 0
        for ii in range(0, 20):
            docs = firestore.Client().collection('Projects').where('project', '==', key).limit(1000).stream()
            for doc in docs:
                doc.reference.delete()
                i += 1
            print(i)
        print(key + ' ' + str((time.time() - t_start) / 60) + ' min ' + str(i))


def add_token_mapbox(token):
    record = dict(id='token_mapbox',
                  token=token)
    firestore.Client().collection('Graphs').document(record['id']).set(record)


def from_rd(x: int, y: int) -> tuple:
    x0 = 155000
    y0 = 463000
    phi0 = 52.15517440
    lam0 = 5.38720621

    # Coefficients or the conversion from RD to WGS84
    Kp = [0, 2, 0, 2, 0, 2, 1, 4, 2, 4, 1]
    Kq = [1, 0, 2, 1, 3, 2, 0, 0, 3, 1, 1]
    Kpq = [3235.65389, -32.58297, -0.24750, -0.84978, -0.06550, -0.01709,
           -0.00738, 0.00530, -0.00039, 0.00033, -0.00012]

    Lp = [1, 1, 1, 3, 1, 3, 0, 3, 1, 0, 2, 5]
    Lq = [0, 1, 2, 0, 3, 1, 1, 2, 4, 2, 0, 0]
    Lpq = [5260.52916, 105.94684, 2.45656, -0.81885, 0.05594, -0.05607,
           0.01199, -0.00256, 0.00128, 0.00022, -0.00022, 0.00026]

    """
    Converts RD coordinates into WGS84 coordinates
    """
    dx = 1E-5 * (x - x0)
    dy = 1E-5 * (y - y0)
    latitude = phi0 + sum([v * dx ** Kp[i] * dy ** Kq[i]
                           for i, v in enumerate(Kpq)]) / 3600
    longitude = lam0 + sum([v * dx ** Lp[i] * dy ** Lq[i]
                            for i, v in enumerate(Lpq)]) / 3600
    return latitude, longitude


def set_date_update():
    record = dict(id='update_date', date=pd.datetime.now().strftime('%Y-%m-%d'))
    firestore.Client().collection('Graphs').document(record['id']).set(record)


def error_check_FCBC(df_l):
    errors_FC_BC = {}
    for key in df_l:
        df = df_l[key]
        errors_FC_BC[key] = {}
        if not df.empty:
            errors_FC_BC[key]['101'] = df[df.kabelid.isna() & ~df.opleverdatum.isna() & (df.postcode.isna() |
                                          df.huisnummer.isna())].sleutel.to_list()
            errors_FC_BC[key]['102'] = df[df.plandatum.isna()].sleutel.to_list()
            errors_FC_BC[key]['103'] = df[df.opleverdatum.isna() &
                                          df.opleverstatus.isin(['2', '10', '90', '91', '96', '97', '98', '99'])].sleutel.to_list()
            errors_FC_BC[key]['104'] = df[df.opleverstatus.isna()].sleutel.to_list()
            # errors_FC_BC[key]['114'] = df[df.toestemming.isna()].sleutel.to_list()
            errors_FC_BC[key]['115'] = errors_FC_BC[key]['118'] = df[df.soort_bouw.isna()].sleutel.to_list()  # soort_bouw hoort bij?
            errors_FC_BC[key]['116'] = df[df.ftu_type.isna()].sleutel.to_list()
            errors_FC_BC[key]['117'] = df[df['toelichting_status'].isna() & df.opleverstatus.isin(['4', '12'])].sleutel.to_list()
            errors_FC_BC[key]['119'] = df[df['toelichting_status'].isna() & df.redenna.isin(['R8', 'R9', 'R17'])].sleutel.to_list()

            errors_FC_BC[key]['120'] = []  # doorvoerafhankelijk niet aanwezig
            errors_FC_BC[key]['121'] = df[(df.postcode.isna() & ~df.huisnummer.isna()) |
                                          (~df.postcode.isna() & df.huisnummer.isna())].sleutel.to_list()
            errors_FC_BC[key]['122'] = df[~((df.kast.isna() & df.kastrij.isna() & df.odfpos.isna() &  # kloppen deze velden?
                                            df.catvpos.isna() & df.odf.isna()) |
                                            (~df.kast.isna() & ~df.kastrij.isna() & ~df.odfpos.isna() &
                                            ~df.catvpos.isna() & ~df.areapop.isna() & ~df.odf.isna()))].sleutel.to_list()
            errors_FC_BC[key]['123'] = df[df.projectcode.isna()].sleutel.to_list()
            errors_FC_BC[key]['301'] = df[~df.opleverdatum.isna() & df.opleverstatus.isin(['0', '14'])].sleutel.to_list()
            errors_FC_BC[key]['303'] = df[df.kabelid.isna() & (df.postcode.isna() | df.huisnummer.isna())].sleutel.to_list()
            errors_FC_BC[key]['304'] = []  # geen column Kavel...
            errors_FC_BC[key]['306'] = df[~df.kabelid.isna() &
                                          df.opleverstatus.isin(['90', '91', '96', '97', '98', '99'])].sleutel.to_list()
            errors_FC_BC[key]['308'] = []  # geen HLopleverdatum...
            errors_FC_BC[key]['309'] = []  # geen doorvoerafhankelijk aanwezig...

            errors_FC_BC[key]['310'] = []  # df[~df.KabelID.isna() & df.Areapop.isna()].sleutel.to_list()  # strengID != KabelID?
            errors_FC_BC[key]['311'] = df[df.redenna.isna() & ~df.opleverstatus.isin(['2', '10', '50'])].sleutel.to_list()
            errors_FC_BC[key]['501'] = [df.sleutel[el] for el in df[~df.postcode.isna()].index if (len(df.postcode[el]) != 6) |
                                                                                                  (not df.postcode[el][0:4].isnumeric()) |
                                                                                                  (df.postcode[el][4].isnumeric()) |
                                                                                                  (df.postcode[el][5].isnumeric())]
            errors_FC_BC[key]['502'] = []  # niet te checken, geen toegang tot CLR
            errors_FC_BC[key]['503'] = []  # date is already present in different format...yyyy-mm-dd??
            errors_FC_BC[key]['504'] = []  # date is already present in different format...yyyy-mm-dd??
            errors_FC_BC[key]['506'] = df[~df.opleverstatus.isin(['0', '1', '2', '4', '5', '6', '7,' '8', '9', '10', '11', '12', '13',
                                                                  '14', '15', '30', '31', '33', '34', '35', '50', '90', '91', '96',
                                                                  '97', '98', '99'])].sleutel.to_list()
            errors_FC_BC[key]['508'] = []  # niet te checken, geen toegang tot Areapop
            errors_FC_BC[key]['509'] = [df.sleutel[el] for el in df[~df.kastrij.isna()].index if (len(df.kastrij[el]) > 2) |
                                                                                                 (len(df.kastrij[el]) < 1) |
                                                                                                 (not df.kastrij[el].isnumeric())]
            errors_FC_BC[key]['510'] = [df.sleutel[el] for el in df[~df.kast.isna()].index if (len(df.kast[el]) > 4) |
                                                                                              (len(df.kast[el]) < 1) |
                                                                                              (not df.kast[el].isnumeric())]

            errors_FC_BC[key]['511'] = [df.sleutel[el] for el in df[~df.odf.isna()].index if (len(df.odf[el]) > 5) |
                                                                                             (len(df.odf[el]) < 1) |
                                                                                             (not df.odf[el].isnumeric())]
            errors_FC_BC[key]['512'] = [df.sleutel[el] for el in df[~df.odfpos.isna()].index if (len(df.odfpos[el]) > 2) |
                                                                                                (len(df.odfpos[el]) < 1) |
                                                                                                (not df.odfpos[el].isnumeric())]
            errors_FC_BC[key]['513'] = [df.sleutel[el] for el in df[~df.catv.isna()].index if (len(df.catv[el]) > 5) |
                                                                                              (len(df.catv[el]) < 1) |
                                                                                              (not df.catv[el].isnumeric())]
            errors_FC_BC[key]['514'] = [df.sleutel[el] for el in df[~df.catvpos.isna()].index if (len(df.catvpos[el]) > 3) |
                                                                                                 (len(df.catvpos[el]) < 1) |
                                                                                                 (not df.catvpos[el].isnumeric())]
            errors_FC_BC[key]['516'] = [df.sleutel[el] for el in df[df.projectcode.isna()].index
                                        if (not str(df.projectcode[el]).isnumeric()) & (~pd.isnull(df.projectcode[el]))]  # cannot check
            errors_FC_BC[key]['517'] = []  # date is already present in different format...yyyy-mm-dd??
            errors_FC_BC[key]['518'] = df[~df.toestemming.isin(['Ja', 'Nee', np.nan])].sleutel.to_list()
            errors_FC_BC[key]['519'] = df[~df.soort_bouw.isin(['Laag', 'Hoog', 'Duplex', 'Woonboot', 'Onbekend'])].sleutel.to_list()
            errors_FC_BC[key]['520'] = df[(df.ftu_type.isna() & df.opleverstatus.isin(['2', '10'])) |
                                          (~df.ftu_type.isin(['FTU_GN01', 'FTU_GN02', 'FTU_PF01', 'FTU_PF02',
                                                              'FTU_TY01', 'FTU_ZS_GN01', 'FTU_TK01', 'Onbekend']))].sleutel.to_list()
            errors_FC_BC[key]['521'] = [df.sleutel[el] for el in df[~df['toelichting_status'].isna()]['toelichting_status'].index
                                        if len(df[~df['toelichting_status'].isna()]['toelichting_status'][el]) < 3]

            errors_FC_BC[key]['522'] = []  # Civieldatum not present in our FC dump
            errors_FC_BC[key]['524'] = []  # Kavel not present in our FC dump
            errors_FC_BC[key]['527'] = []  # HL opleverdatum not present in our FC dump
            errors_FC_BC[key]['528'] = df[~df.redenna.isin([np.nan, 'R0', 'R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'R9',
                                                            'R10', 'R11', 'R12', 'R13', 'R14', 'R15', 'R16', 'R17', 'R18', 'R19',
                                                            'R20', 'R21', 'R22'])].sleutel.to_list()
            errors_FC_BC[key]['531'] = []  # strengID niet aanwezig in deze FCdump
            # if df[~df.CATVpos.isin(['999'])].shape[0] > 0:
            #     errors_FC_BC[key]['532'] = [df.sleutel[el] for el in df.ODFpos.index
            #                                 if ((int(df.CATVpos[el]) - int(df.ODFpos[el]) != 1) &
            #                                     (int(df.CATVpos[el]) != '999')) |
            #                                    (int(df.ODFpos[el]) % 2 == [])]
            errors_FC_BC[key]['533'] = []  # Doorvoerafhankelijkheid niet aanwezig in deze FCdump
            errors_FC_BC[key]['534'] = []  # geen toegang tot CLR om te kunnen checken
            errors_FC_BC[key]['535'] = [df.sleutel[el] for el in df[~df['toelichting_status'].isna()]['toelichting_status'].index
                                        if ',' in df['toelichting_status'][el]]
            errors_FC_BC[key]['536'] = [df.sleutel[el] for el in df[~df.kabelid.isna()].kabelid.index if len(df.kabelid[el]) < 3]

            errors_FC_BC[key]['537'] = []  # Blok not present in our FC dump
            errors_FC_BC[key]['701'] = []  # Kan niet gecheckt worden, hebben we vorige waarde voor nodig...
            errors_FC_BC[key]['702'] = df[~df.odf.isna() & df.opleverstatus.isin(['90', '91', '96', '97', '98', '99'])].sleutel.to_list()
            errors_FC_BC[key]['707'] = []  # Kan niet gecheckt worden, hebben we vorige waarde voor nodig...
            errors_FC_BC[key]['708'] = df[(df.opleverstatus.isin(['90']) & ~df.redenna.isin(['R15', 'R16', 'R17'])) |
                                          (df.opleverstatus.isin(['91']) &
                                          ~df.redenna.isin(['R12', 'R13', 'R14', 'R21']))].sleutel.to_list()
            # errors_FC_BC[key]['709'] = df[(df.ODF + df.ODFpos).duplicated(keep='last')].sleutel.to_list()  # klopt dit?
            errors_FC_BC[key]['710'] = df[(df.kabelid + df.adres).duplicated()].sleutel.to_list()
            # errors_FC_BC[key]['711'] = df[~df.CATV.isin(['999']) | ~df.CATVpos.isin(['999'])].sleutel.to_list()  # wanneer PoP 999?
            errors_FC_BC[key]['713'] = []  # type bouw zit niet in onze FC dump
            # if df[df.ODF.isin(['999']) & df.ODFpos.isin(['999']) & df.CATVpos.isin(['999']) & df.CATVpos.isin(['999'])].shape[0] > 0:
            #     errors_FC_BC[key]['714'] = df[~df.ODF.isin(['999']) | ~df.ODFpos.isin(['999']) | ~df.CATVpos.isin(['999']) |
            #                                 ~df.CATVpos.isin(['999'])].sleutel.to_list()

            errors_FC_BC[key]['716'] = []  # niet te checken, geen toegang tot SIMA
            errors_FC_BC[key]['717'] = []  # type bouw zit niet in onze FC dump
            errors_FC_BC[key]['719'] = []  # kan alleen gecheckt worden met geschiedenis
            errors_FC_BC[key]['721'] = []  # niet te checken, geen Doorvoerafhankelijkheid in FC dump
            errors_FC_BC[key]['723'] = df[(df.redenna.isin(['R15', 'R16', 'R17']) & ~df.opleverstatus.isin(['90'])) |
                                          (df.redenna.isin(['R12', 'R12', 'R14', 'R21']) & ~df.opleverstatus.isin(['91'])) |
                                          (df.opleverstatus.isin(['90']) & df.redenna.isin(['R2', 'R11']))].sleutel.to_list()
            errors_FC_BC[key]['724'] = df[(~df.opleverdatum.isna() & df.redenna.isin(['R0', 'R19', 'R22']))].sleutel.to_list()
            errors_FC_BC[key]['725'] = []  # geen zicht op vraagbundelingsproject of niet
            errors_FC_BC[key]['726'] = []  # niet te checken, geen HLopleverdatum aanwezig
            errors_FC_BC[key]['727'] = df[df.opleverstatus.isin(['50'])].sleutel.to_list()
            errors_FC_BC[key]['728'] = []  # voorkennis nodig over poptype

            errors_FC_BC[key]['729'] = []  # kan niet checken, vorige staat FC voor nodig
            errors_FC_BC[key]['90x'] = []  # kan niet checken, extra info over bestand nodig!

    n_err = {}
    for key in errors_FC_BC:
        err_all = []
        for key2 in errors_FC_BC[key]:
            for el in errors_FC_BC[key][key2]:
                if el not in err_all:
                    err_all += [el]
        n_err[key] = len(err_all)

    return n_err, errors_FC_BC


def cluster_reden_na(label, clusters):
    for k, v in clusters.items():
        if label in v:
            return k


def pie_chart_reden_na(df_na, clusters, key):

    df_na['cluster_redenna'] = df_na['redenna'].apply(lambda x: cluster_reden_na(x, clusters))
    df_na.loc[df_na['opleverstatus'] == '2', ['cluster_redenna']] = 'HC'

    df_na = df_na.groupby('cluster_redenna').size()
    df_na = df_na.to_frame(name='count').reset_index()
    labels = df_na['cluster_redenna'].tolist()
    values = df_na['count'].tolist()

    data = {
                'labels': labels,
                'values': values,
                'marker': {
                            'colors':
                            [
                                'rgb(0, 204, 0)',
                                'rgb(255, 255, 0)',
                                'rgb(204, 0, 0)'
                            ]
                          }
           }
    document = 'pie_na_' + key
    return data, document


def overview_reden_na(df_l, clusters):
    full_df = pd.concat(df_l.values())
    data, document = pie_chart_reden_na(full_df, clusters, 'overview')
    layout = get_pie_layout()
    fig = {
                'data': data,
                'layout': layout
          }
    record = dict(id=document, figure=fig)
    return record


def individual_reden_na(df_l, clusters):
    record_dict = {}
    for project, df in df_l.items():
        data, document = pie_chart_reden_na(df, clusters, project)
        layout = get_pie_layout()
        fig = {
                'data': data,
                'layout': layout
            }
        record = dict(id=document, figure=fig)
        record_dict[document] = record
    return record_dict


def to_firestore(collection, document, record):
    firestore.Client().collection(collection).document(document).set(record)


def get_pie_layout():
    layout = {
                #   'clickmode': 'event+select',
                'showlegend': True,
                'margin': {'l': 5, 'r': 15, 'b': 5, 't': 40},
                'title': {'text': 'Opgegeven reden na'},
                'height': 350,
             }
    return layout
