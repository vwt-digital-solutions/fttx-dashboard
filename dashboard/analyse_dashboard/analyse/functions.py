import warnings
from collections import defaultdict
from typing import NamedTuple

import pandas as pd
import numpy as np
from google.cloud import firestore, storage
import os
import time
import json
import datetime
import hashlib
import config
from collections import namedtuple
from pandas.api.types import CategoricalDtype

colors = config.colors_vwt


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
            df['title'] = key = 'Bergen op Zoom Noord  wijk 01 + Halsteren'
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
                           'SchouwDatum': 'schouwdatum', 'Plan Status': 'plan_status'}, inplace=True)
        if flag == 0:
            df = df[col]
        df.loc[~df['opleverdatum'].isna(), ('opleverdatum')] = \
            [el[6:10] + '-' + el[3:5] + '-' + el[0:2] for el in df[~df['opleverdatum'].isna()]['opleverdatum']]
        df.loc[~df['hasdatum'].isna(), ('hasdatum')] = \
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


def extract_data_planning(path_data):
    # if 'gs://' in path_data:
    #     xls = pd.ExcelFile(path_data)
    # else:
    xls = pd.ExcelFile(path_data)
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
                HP['Bergen op Zoom Noord Halsteren'] = HP.pop(df.loc[el, ('Unnamed: 0')])
            if df.loc[el, ('Unnamed: 0')] == 'Den Haag Bezuidenhout':
                HP['Den Haag Bezuidenhout'] = HP.pop(df.loc[el, ('Unnamed: 0')])
            if df.loc[el, ('Unnamed: 0')] == 'Den Haag Morgenstond':
                HP['Den Haag Morgenstond west'] = HP.pop(df.loc[el, ('Unnamed: 0')])
            if df.loc[el, ('Unnamed: 0')] == 'Den Haag Vrederust Bouwlust':
                HP['Den Haag Vredelust Bouwlust'] = HP.pop(df.loc[el, ('Unnamed: 0')])
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
    doc = firestore.Client().collection('Data').document('analysis').get().to_dict()
    if doc is not None:
        date_FTU0 = doc['FTU0']
        date_FTU1 = doc['FTU1']
        dates = date_FTU0, date_FTU1
    else:
        print("Could not retrieve FTU0 and FTU1 from firestore, setting from original file")
        dates = get_data_targets_init(path_data)
    return dates


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
        'Bergen op Zoom Noord  wijk 01 + Halsteren': 'Bergen op Zoom Noord Halsteren',  # niet in FC
        'Nijmegen Dukenburg': 'Nijmegen Dukenburg',  # niet in FC
        'Den Haag - Haagse Hout-Bezuidenhout West': 'Den Haag Bezuidenhout',  # niet in FC??
        'Den Haag - Vrederust en Bouwlust': 'Den Haag Vredelust Bouwlust',  # niet in FC??
        'Gouda Kort Haarlem en Noord': 'KPN Gouda Kort Haarlem en Noord',
        # wel in FC, geen FT0 of FT1, niet afgerond, niet actief in FC...
        # Den Haag Cluster B (geen KPN), Den Haag Regentessekwatier (ON HOLD), Den Haag (??)
        # afgerond in FC...FTU0/FTU1 schatten
        # Arnhem Marlburgen, Arnhem Spijkerbuurt, Bavel, Brielle, Helvoirt, LCM project
    }
    df_targetsKPN = pd.read_excel(path_data, sheet_name='KPN')
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
    fn_teams = path_data + 'Weekrapportage FttX projecten - Week 22-2020.xlsx'
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


def get_start_time(df: pd.DataFrame):
    # What does t_s stand for? Would prefer to use a descriptive variable name.
    t_s = {}
    for project, project_df in df.groupby("project"):
        start_time = project_df.opleverdatum.min()
        if start_time is pd.NaT:
            t_s[project] = pd.to_datetime(pd.Timestamp.now().strftime('%Y-%m-%d'))
        else:
            t_s[project] = start_time
    return t_s


def get_timeline(t_s):
    x_axis = pd.date_range(min(t_s.values()), periods=1000 + 1, freq='D')
    return x_axis


def get_total_objects(df):  # Don't think this is necessary to calculate at this point, should be done later.
    total_objects = df[['sleutel', 'project']].groupby(by="project").count().to_dict()['sleutel']
    # This hardcoded stuff can lead to unexpected behaviour. Should this still be in here?
    # total_objects['Bergen op Zoom Noord Halsteren'] = 9465  # not yet in FC, total from excel bouwstromen
    # total_objects['Den Haag Bezuidenhout'] = 9488  # not yet in FC, total from excel bouwstromen
    # total_objects['Den Haag Vredelust Bouwlust'] = 11918  # not yet in FC, total from excel bouwstromen
    return total_objects


# Function that adds columns to the source data, to be used in project specs
# hpend is a boolean column indicating whether an object has been delivered
# homes_completed is a boolean column indicating a home has been completed
# bis_gereed is a boolean column indicating whther the BIS for an object has been finished
def add_relevant_columns(df: pd.DataFrame, year):
    # TODO add to tranform part of the ETL
    if not year:
        year = str(pd.Timestamp.now().year)
    start_year = pd.to_datetime(year + '-01-01')
    end_year = pd.to_datetime(year + '-12-31')
    df['hpend'] = df.opleverdatum.apply(lambda x: (x >= start_year) and (x <= end_year))
    df['homes_completed'] = (df.opleverstatus == '2') & (df.hpend)
    df['homes_completed_total'] = (df.opleverstatus == '2')
    df['bis_gereed'] = df.opleverstatus != '0'
    return df


# Calculates the amount of homes completed per project in a dictionary
def get_homes_completed(df: pd.DataFrame):
    result = df[['project', 'homes_completed']] \
        .groupby(by="project") \
        .sum() \
        .reset_index() \
        .set_index("project") \
        .to_dict()['homes_completed']

    return result

    # return {k: sum(v.homes_completed) for k, v in df_l.items()}


# Calculate the amount of objects per project that have been
# Permanently passed or completed
def get_HPend_2020(df: pd.DataFrame):
    result = df[['project', 'hpend']] \
        .groupby(by="project") \
        .sum() \
        .reset_index() \
        .set_index("project") \
        .to_dict()['hpend']

    return result


def get_HPend(df: pd.DataFrame):
    test_df = df[['project']].copy()
    test_df["hpend_not_2020"] = df.opleverdatum.notna()
    return test_df.groupby(by="project").sum().reset_index().set_index("project").to_dict()['hpend_not_2020']


# Objects that are ready for HAS
# These are obejcts for which:
# - a permission has been filled in (granted or rejected)
# - The BIS (basic infrastructure) is in place
def get_has_ready(df: pd.DataFrame):
    tmp_df = df.copy()
    tmp_df['has_ready'] = ~df.toestemming.isna() & df.bis_gereed
    result = tmp_df[['project', 'has_ready']] \
        .groupby(by="project") \
        .sum() \
        .reset_index() \
        .set_index("project") \
        .to_dict()['has_ready']

    return result


# Total ratio of completed objects v.s. completed + permanently passed objects.
def get_hc_hpend_ratio_total(hc, hpend):
    return round(sum(hc.values()) / sum(hpend.values()), 2)


# Calculates the ratio between homes completed v.s. completed + permanently passed objects per project
def get_hc_hpend_ratio(df: pd.DataFrame):
    temp_df = df[['sleutel', "project", 'homes_completed_total']].copy()
    temp_df['has_opleverdatum'] = ~df.opleverdatum.isna()
    sum_df = temp_df[['sleutel', "project", "has_opleverdatum", 'homes_completed_total']].groupby(by="project").sum().reset_index()
    sum_df['ratio'] = sum_df.apply(
        lambda x: x.homes_completed_total / x.has_opleverdatum * 100
        if x.has_opleverdatum else 0, axis=1
    )
    return sum_df[['project', 'ratio']].set_index("project").to_dict()['ratio']


def get_has_werkvoorraad(df: pd.DataFrame):
    # todo add in_has_werkvoorraad column in etl and use that column
    return len(df[
                   (~df.toestemming.isna()) &
                   (df.opleverstatus != '0') &
                   (df.opleverdatum.isna())
                   ])


# Function to add relevant data to the source data_frames
def preprocess_data(df, year):
    df = add_relevant_columns(df, year)
    return df


class ProjectSpecs(NamedTuple):
    hc_hp_end_ratio_total: object
    hc_hpend_ratio: object
    has_ready: object
    homes_ended: object
    werkvoorraad: object


def calculate_projectspecs(df: pd.DataFrame) -> ProjectSpecs:
    homes_completed = get_homes_completed(df)
    homes_ended_2020 = get_HPend_2020(df)
    homes_ended = get_HPend(df)
    has_ready = get_has_ready(df)
    hc_hpend_ratio = get_hc_hpend_ratio(df)
    hc_hp_end_ratio_total = get_hc_hpend_ratio_total(homes_completed, homes_ended_2020)
    werkvoorraad = get_has_werkvoorraad(df)

    return ProjectSpecs(hc_hp_end_ratio_total, hc_hpend_ratio, has_ready, homes_ended, werkvoorraad)


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

    TargetResults = namedtuple("TargetResults", ['y_target_l', 't_diff'])
    return TargetResults(y_target_l, t_diff)


def prognose(df: pd.DataFrame, t_s, x_d, tot_l, date_FTU0):
    x_prog = np.array(list(range(0, len(x_d))))
    cutoff = 85

    rc1 = {}
    rc2 = {}
    d_real_l = {}
    t_shift = {}
    y_prog_l = {}
    for project, project_df in df.groupby(by="project"):  # to calculate prognoses for projects in FC
        d_real = project_df[~project_df['opleverdatum'].isna()]
        if not d_real.empty:
            d_real = d_real.groupby(['opleverdatum']).agg({'sleutel': 'count'}).rename(columns={'sleutel': 'Aantal'})
            d_real.index = pd.to_datetime(d_real.index, format='%Y-%m-%d')
            d_real = d_real.sort_index()
            d_real = d_real[d_real.index < pd.Timestamp.now()]

            d_real = d_real.cumsum() / tot_l[project] * 100
            d_real[d_real.Aantal > 100] = 100  # only necessary for DH
            t_shift[project] = (d_real.index.min() - min(t_s.values())).days
            d_real.index = (d_real.index - d_real.index[0]).days + t_shift[project]
            d_real_l[project] = d_real

            d_rc1 = d_real[d_real.Aantal < cutoff]
            if len(d_rc1) > 1:
                rc1[project], b1 = np.polyfit(d_rc1.index, d_rc1, 1)
                y_prog1 = b1[0] + rc1[project][0] * x_prog
                y_prog_l[project] = y_prog1.copy()

            d_rc2 = d_real[d_real.Aantal >= cutoff]
            if (len(d_rc2) > 1) & (len(d_rc1) > 1):
                rc2[project], b2 = np.polyfit(d_rc2.index, d_rc2, 1)
                y_prog2 = b2[0] + rc2[project][0] * x_prog
                x_i, y_i = get_intersect([x_prog[0], y_prog1[0]], [x_prog[-1], y_prog1[-1]],
                                         [x_prog[0], y_prog2[0]], [x_prog[-1], y_prog2[-1]])
                y_prog_l[project][x_prog >= x_i] = y_prog2[x_prog >= x_i]
            # if (len(d_rc2) > 1) & (len(d_rc1) <= 1):
            #     rc1[project], b1 = np.polyfit(d_rc2.index, d_rc2, 1)
            #     y_prog1 = b1[0] + rc1[project][0] * x_prog
            #     y_prog_l[project] = y_prog1.copy()

    rc1_mean = sum(rc1.values()) / len(rc1.values())
    rc2_mean = sum(rc2.values()) / len(rc2.values())
    for project, project_df in df.groupby(by="project"):
        if (project in rc1) & (project not in rc2):  # the case of 2 realisation dates, rc1 but no rc2
            if max(y_prog_l[project]) > cutoff:
                b2_mean = cutoff - (rc2_mean * x_prog[y_prog_l[project] >= cutoff][0])
                y_prog2 = b2_mean + rc2_mean * x_prog
                y_prog_l[project][y_prog_l[project] >= cutoff] = y_prog2[y_prog_l[project] >= cutoff]
        if (project in d_real_l) & (project not in y_prog_l):  # the case of only 1 realisation date
            b1_mean = -(rc1_mean * t_shift[project])
            y_prog1 = b1_mean + rc1_mean * x_prog
            b2_mean = cutoff - (rc2_mean * x_prog[y_prog1 >= cutoff][0])
            y_prog2 = b2_mean + rc2_mean * x_prog
            y_prog_l[project] = y_prog1.copy()
            y_prog_l[project][y_prog1 >= cutoff] = y_prog2[y_prog1 >= cutoff]
        if project not in d_real_l:  # the case of no realisation date
            t_shift[project] = x_prog[x_d == pd.Timestamp.now().strftime('%Y-%m-%d')][0]
            if project in date_FTU0:
                if not pd.isnull(date_FTU0[project]):
                    t_shift[project] = x_prog[x_d == date_FTU0[project]][0]
            b1_mean = -(rc1_mean * (t_shift[project] + 14))  # to include delay of two week
            y_prog1 = b1_mean + rc1_mean * x_prog
            b2_mean = cutoff - (rc2_mean * x_prog[y_prog1 >= cutoff][0])
            y_prog2 = b2_mean + rc2_mean * x_prog
            y_prog_l[project] = y_prog1.copy()
            y_prog_l[project][y_prog1 >= cutoff] = y_prog2[y_prog1 >= cutoff]

    for project in y_prog_l:
        y_prog_l[project][y_prog_l[project] > 100] = 100
        y_prog_l[project][y_prog_l[project] < 0] = 0

    PrognoseResult = namedtuple("PrognoseResult", ['rc1', 'rc2', 'd_real_l', 'y_prog_l', 'x_prog', 't_shift', 'cutoff'])
    return PrognoseResult(rc1, rc2, d_real_l, y_prog_l, x_prog, t_shift, cutoff)


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

    OverviewResults = namedtuple("OverviewResults", ['df_prog', 'df_target', 'df_real', 'df_plan'])
    return OverviewResults(df_prog, df_target, df_real, df_plan)


def graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad, res):
    if 'W' in res:
        n_now = int((pd.Timestamp.now() - pd.to_datetime('2019-12-30')).days / 7) + 1
        n_d = int((pd.Timestamp.now() - pd.to_datetime('2020-' + str(datetime.date.today().month) + '-01')).days / 7)
        x_ticks = list(range(n_now - n_d, n_now + 5 - n_d))
        x_ticks_text = [datetime.datetime.strptime('2020-W' + str(int(el - 1)) + '-1', "%Y-W%W-%w").date().strftime(
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

    prog0 = df_prog[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d']
    prog = prog0.to_list()
    target0 = df_target[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d']
    target = target0.to_list()
    real0 = df_real[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d']
    real = real0.to_list()
    plan0 = df_plan[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d']
    plan = plan0.to_list()
    plan[0:n_now] = real[0:n_now]  # gelijk trekken afgelopen periode

    if 'M' == res:
        jaaroverzicht = dict(id='jaaroverzicht', target=str(round(sum(target[1:]))), real=str(round(sum(real[1:]))),
                             plan=str(round(sum(plan[n_now:]) - real[n_now])),
                             prog=str(round(sum(prog[n_now:]) - real[n_now])),
                             HC_HPend=str(HC_HPend), HAS_werkvoorraad=str(HAS_werkvoorraad), prog_c='pretty_container')
        if jaaroverzicht['prog'] < jaaroverzicht['plan']:
            jaaroverzicht['prog_c'] = 'pretty_container_red'

    bar_now = dict(x=[n_now],
                   y=[y_range[1]],
                   name='Huidige week',
                   type='bar',
                   marker=dict(color=colors['black']),
                   width=0.5 * width,
                   )
    bar_t = dict(x=[el - 0.5 * width for el in x],
                 y=target,
                 name='Outlook (KPN)',
                 type='bar',
                 marker=dict(color=colors['lightgray']),
                 width=width,
                 )
    bar_pr = dict(x=x,
                  y=prog,
                  name='Voorspelling (VQD)',
                  mode='markers',
                  marker=dict(color=colors['yellow'], symbol='diamond', size=15),
                  #   width=0.2,
                  )
    bar_r = dict(x=[el + 0.5 * width for el in x],
                 y=real,
                 name='Realisatie (FC)',
                 type='bar',
                 marker=dict(color=colors['green']),
                 width=width,
                 )
    bar_pl = dict(x=x,
                  y=plan,
                  name='Planning HP (VWT)',
                  type='lines',
                  marker=dict(color=colors['red']),
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
            'margin': {'l': 5, 'r': 15, 'b': 10, 't': 40},
            'title': {'text': text_title},
            'xaxis': {'range': x_range,
                      'tickvals': x_ticks,
                      'ticktext': x_ticks_text,
                      'title': ' '},
            'yaxis': {'range': y_range, 'title': 'Aantal HPend'},
            'plot_bgcolor': colors['plot_bgcolor'],
            'paper_bgcolor': colors['paper_bgcolor'],
            #   'annotations': [dict(x=x_ann, y=y_ann, text=jaaroverzicht, xref="x", yref="y",
            #                   ax=0, ay=0, alignment='left', font=dict(color="black", size=15))]
        },
    }

    prog0.index = prog0.index.strftime('%Y-%m-%d')
    data_pr = dict(count_voorspellingdatum=prog0.to_dict())
    target0.index = target0.index.strftime('%Y-%m-%d')
    data_t = dict(count_outlookdatum=target0.to_dict())
    real0.index = real0.index.strftime('%Y-%m-%d')
    data_r = dict(count_opleverdatum=real0.to_dict())
    plan0.index = plan0.index.strftime('%Y-%m-%d')
    data_p = dict(count_hasdatum=plan0.to_dict())
    if 'W' in res:
        record = dict(id='graph_targets_W', figure=fig)
        return record, data_pr, data_t, data_r, data_p
    if 'M' == res:
        record = dict(id='graph_targets_M', figure=fig)
        return record, data_pr, data_t, data_r, data_p


def slice_for_jaaroverzicht(data):
    res = 'M'
    close = 'left'
    loff = None
    period = ['2019-12-23', '2020-12-27']
    return data[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d'].to_list()


def preprocess_for_jaaroverzicht(*args):
    return [slice_for_jaaroverzicht(arg) for arg in args]
    # prog = slice_for_jaaroverzicht(df_prog)
    # target = slice_for_jaaroverzicht(df_target)
    # real = slice_for_jaaroverzicht(df_real)
    # plan = slice_for_jaaroverzicht(df_plan)
    # return prog, target, real, plan


def calculate_jaaroverzicht(prognose, target, realisatie, planning, HAS_werkvoorraad, HC_HPend):
    n_now = datetime.date.today().month
    planning[0:n_now] = realisatie[0:n_now]  # gelijk trekken afgelopen periode

    target_sum = str(round(sum(target[1:])))
    realisatie_now = realisatie[n_now]
    planning_sum = sum(planning[n_now:])
    prognose_sum = sum(prognose[n_now:])
    planning_result = planning_sum - realisatie_now
    prognose_result = prognose_sum - realisatie_now
    realisatie_sum = str(round(sum(realisatie[1:])))

    jaaroverzicht = dict(id='jaaroverzicht',
                         target=str(int(target_sum)),
                         real=str(int(realisatie_sum)),
                         plan=str(int(planning_result)),
                         prog=str(int(prognose_result)),
                         HC_HPend=str(HC_HPend),
                         HAS_werkvoorraad=str(int(HAS_werkvoorraad)),
                         prog_c='pretty_container')
    if jaaroverzicht['prog'] < jaaroverzicht['plan']:
        jaaroverzicht['prog_c'] = 'pretty_container_red'
    return jaaroverzicht


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
        y_schouw[key] = ((w_SLB + w_SHB).cumsum() / tot_l[key] * 100).to_list() + [0] * (
                len(y_target[key]) - len(w_SLB))

    m_gegraven = [el1 + el2 for el1, el2 in zip(m_BIS_all, m_HAS_all)]
    rc_BIS, _ = np.polyfit(teams_BIS_all, m_gegraven, 1)  # aantal meters BIS per team per week
    rc_HAS, _ = np.polyfit(teams_HAS_all, w_HAS_all, 1)
    w_now = int((pd.Timestamp.now() - pd.to_datetime('2019-12-30')).days / 7) + 1
    advies = {}
    for key in y_target:
        BIS_advies = round(
            (y_target[key][w_now] - y_BIS[key][w_now]) / 100 * tot_l[key] * 7 / rc_BIS)  # gem 7m BIS per woning
        if BIS_advies <= 0:
            BIS_advies = 'On target!'
        HAS_advies = round((y_target[key][w_now] - y_HAS[key][w_now]) / 100 * tot_l[key] / rc_HAS)
        if HAS_advies <= 0:
            HAS_advies = 'On target!'
        advies[key] = 'Advies:<br>' + 'BIS teams: ' + str(BIS_advies) + '<br>HAS teams: ' + str(HAS_advies)

    return x_target, y_target, y_BIS, y_HASm, y_HAS, y_schouw, advies


def prognose_graph(x_d, y_prog_l, d_real_l, y_target_l):
    record_dict = {}
    for key in y_prog_l:
        fig = {'data': [{
            'x': list(x_d.strftime('%Y-%m-%d')),
            'y': list(y_prog_l[key]),
            'mode': 'lines',
            'line': dict(color=colors['yellow']),
            'name': 'Voorspelling (VQD)',
        }],
            'layout': {
                'xaxis': {'title': 'Opleverdatum [d]', 'range': ['2020-01-01', '2020-12-31']},
                'yaxis': {'title': 'Opgeleverd HPend [%]', 'range': [0, 110]},
                'title': {'text': 'Voortgang project vs outlook KPN:'},
                'showlegend': True,
                'legend': {'x': 1.2, 'xanchor': 'right', 'y': 1},
                'height': 350,
                'plot_bgcolor': colors['plot_bgcolor'],
                'paper_bgcolor': colors['paper_bgcolor'],
            },
        }
        if key in d_real_l:
            fig['data'] = fig['data'] + [{
                'x': list(x_d[d_real_l[key].index.to_list()].strftime('%Y-%m-%d')),
                'y': d_real_l[key]['Aantal'].to_list(),
                'mode': 'markers',
                'line': dict(color=colors['green']),
                'name': 'Realisatie (FC)',
            }]

        if key in y_target_l:
            fig['data'] = fig['data'] + [{
                'x': list(x_d.strftime('%Y-%m-%d')),
                'y': list(y_target_l[key]),
                'mode': 'lines',
                'line': dict(color=colors['lightgray']),
                'name': 'Outlook (KPN)',
            }]
        record = dict(id='project_' + key, figure=fig)
        record_dict[key] = record
    return record_dict


def masks_phases(pkey, df_l):
    def calculate_bar(bar_m, mask):
        bar = {}
        for key in bar_m:
            len_b = (bar_m[key] & mask).value_counts()
            if True in len_b:
                bar[key[0:-5]] = str(len_b[True])
            else:
                bar[key[0:-5]] = str(0)
        return bar

    df = df_l[pkey]
    bar_m = {'SchouwenLB0-mask': (df['toestemming'].isna()) &
                                 (df['soort_bouw'] == 'Laag'), 'SchouwenLB1-mask': (~df['toestemming'].isna()) &
                                                                                   (df['soort_bouw'] == 'Laag'),
             'SchouwenHB0-mask': (df['toestemming'].isna()) &
                                 (df['soort_bouw'] != 'Laag'), 'SchouwenHB1-mask': (~df['toestemming'].isna()) &
                                                                                   (df['soort_bouw'] != 'Laag'),
             'BISLB0-mask': (df['opleverstatus'] == '0') &
                            (df['soort_bouw'] == 'Laag'), 'BISLB1-mask': (df['opleverstatus'] != '0') &
                                                                         (df['soort_bouw'] == 'Laag'),
             'BISHB0-mask': (df['opleverstatus'] == '0') &
                            (df['soort_bouw'] != 'Laag'), 'BISHB1-mask': (df['opleverstatus'] != '0') &
                                                                         (df['soort_bouw'] != 'Laag'),
             'Montage-lasDPLB0-mask': (df['laswerkdpgereed'] == '0') &
                                      (df['soort_bouw'] == 'Laag'),
             'Montage-lasDPLB1-mask': (df['laswerkdpgereed'] == '1') &
                                      (df['soort_bouw'] == 'Laag'),
             'Montage-lasDPHB0-mask': (df['laswerkdpgereed'] == '0') &
                                      (df['soort_bouw'] != 'Laag'),
             'Montage-lasDPHB1-mask': (df['laswerkdpgereed'] == '1') &
                                      (df['soort_bouw'] != 'Laag'),
             'Montage-lasAPLB0-mask': (df['laswerkapgereed'] == '0') &
                                      (df['soort_bouw'] == 'Laag'),
             'Montage-lasAPLB1-mask': (df['laswerkapgereed'] == '1') &
                                      (df['soort_bouw'] == 'Laag'),
             'Montage-lasAPHB0-mask': (df['laswerkapgereed'] == '0') &
                                      (df['soort_bouw'] != 'Laag'),
             'Montage-lasAPHB1-mask': (df['laswerkapgereed'] == '1') &
                                      (df['soort_bouw'] != 'Laag'), 'HASLB0-mask': (df['opleverdatum'].isna()) &
                                                                                   (df['soort_bouw'] == 'Laag'),
             'HASLB1-mask': (df['opleverstatus'] == '2') &
                            (df['soort_bouw'] == 'Laag'), 'HASLB1HP-mask': (df['opleverstatus'] != '2') &
                                                                           (~df['opleverdatum'].isna()) &
                                                                           (df['soort_bouw'] == 'Laag'),
             'HASHB0-mask': (df['opleverdatum'].isna()) &
                            (df['soort_bouw'] != 'Laag'), 'HASHB1-mask': (df['opleverstatus'] == '2') &
                                                                         (df['soort_bouw'] != 'Laag'),
             'HASHB1HP-mask': (df['opleverstatus'] != '2') &
                              (~df['opleverdatum'].isna()) &
                              (df['soort_bouw'] != 'Laag')}

    document_list = []
    mask_level0 = True
    bar = calculate_bar(bar_m, mask=mask_level0)
    bar_names = ['0']
    record = dict(bar=bar)
    document = dict(record=record,
                    filter="0",
                    graph_name="status_bar_chart",
                    project=pkey)
    document_list.append(document)

    for key2 in bar_m:
        mask_level1 = bar_m[key2]
        bar = calculate_bar(bar_m, mask=mask_level0 & mask_level1)
        bar_names += ['0' + key2[0:-5]]
        record = dict(bar=bar, mask=json.dumps(df[mask_level0 & mask_level1].sleutel.to_list()))
        document = dict(record=record,
                        filter="0" + key2[0:-5],
                        graph_name="status_bar_chart",
                        project=pkey)
        document_list.append(document)

        for key3 in bar_m:
            mask_level2 = bar_m[key3]
            bar = calculate_bar(bar_m, mask=mask_level0 & mask_level1 & mask_level2)
            bar_names += ['0' + key2[0:-5] + key3[0:-5]]
            record = dict(bar=bar,
                          mask=json.dumps(df[mask_level0 & mask_level1 & mask_level2].sleutel.to_list()))
            document = dict(record=record,
                            filter="0" + key2[0:-5] + key3[0:-5],
                            graph_name="status_bar_chart",
                            project=pkey)
            document_list.append(document)
    return bar_names, document_list


# def set_bar_names(bar_m):
#     bar_names = ['0']
#     for key2 in bar_m:
#         bar_names += ['0' + key2[0:-5]]
#     for key2 in bar_m:
#         for key3 in bar_m:
#             bar_names += ['0' + key2[0:-5] + key3[0:-5]]
#     record = dict(id='bar_names', bar_names=bar_names)
#     firestore.Client().collection('Data').document(record['id']).set(record)


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
        print('Time: ' + str((time.time() - t) / 60) + ' minutes')

    return df_l


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
            'line': {'color': colors['red']}
        },
        {
            'x': [1 / 70 * x_min, 1 / 70 * x_max, 1 / 70 * x_max, 15, 15, 1 / 70 * x_min],
            'y': [y_min, y_min, y_voorraad_p, y_voorraad_p, 150, 150],
            'name': 'Trace 2',
            'mode': 'lines',
            'fill': 'toself',
            'opacity': 1,
            'line': {'color': colors['green']}
        },
        {
            'x': [x_min, 1 / 70 * x_min, 1 / 70 * x_min, 15, 15, 1 / 70 * x_max,
                  1 / 70 * x_max, x_max, x_max, x_min, x_min, 1 / 70 * x_min],
            'y': [y_voorraad_p, y_voorraad_p, 150, 150, y_voorraad_p, y_voorraad_p,
                  y_min, y_min, y_max, y_max, y_voorraad_p, y_voorraad_p],
            'name': 'Trace 2',
            'mode': 'lines',
            'fill': 'toself',
            'opacity': 1,
            'line': {'color': colors['yellow']}
        },
        {
            'x': x,
            'y': y,
            'text': names,
            'name': 'Trace 1',
            'mode': 'markers',
            'marker': {'size': 15, 'color': colors['black']}
        }],
        'layout': {'clickmode': 'event+select',
                   'xaxis': {'title': 'Procent voor of achter HPEnd op KPNTarget', 'range': [x_min, x_max],
                             'zeroline': False},
                   'yaxis': {'title': 'Procent voor of achter op verwachte werkvoorraad', 'range': [y_min, y_max],
                             'zeroline': False},
                   'showlegend': False,
                   'title': {'text': 'Krijg alle projecten in het groene vlak door de pijlen te volgen'},
                   'annotations': [dict(x=-12.5, y=25, ax=0, ay=40, xref="x", yref="y",
                                        text='Verhoog schouw of BIS capaciteit', alignment='left',
                                        showarrow=True, arrowhead=2)] +
                                  [dict(x=12.5, y=25, ax=0, ay=40, xref="x", yref="y",
                                        text='Verhoog schouw of BIS capaciteit', alignment='left',
                                        showarrow=True, arrowhead=2)] +
                                  [dict(x=-13.5, y=160, ax=-100, ay=0, xref="x", yref="y",
                                        text='Verhoog HAS capaciteit',
                                        alignment='left', showarrow=True, arrowhead=2)] +
                                  [dict(x=-13.5, y=40, ax=-100, ay=0, xref="x", yref="y",
                                        text='Verruim afspraak KPN',
                                        alignment='left', showarrow=True, arrowhead=2)] +
                                  [dict(x=13.5, y=160, ax=100, ay=0, xref="x", yref="y",
                                        text='Verlaag HAS capcaciteit',
                                        alignment='right', showarrow=True, arrowhead=2)] +
                                  [dict(x=13.5, y=40, ax=100, ay=0, xref="x", yref="y",
                                        text='Verscherp afspraak KPN',
                                        alignment='right', showarrow=True, arrowhead=2)] +
                                  [dict(x=12.5, y=185, ax=0, ay=-40, xref="x", yref="y",
                                        text='Verlaag schouw of BIS capaciteit', alignment='left',
                                        showarrow=True, arrowhead=2)] +
                                  [dict(x=-12.5, y=185, ax=0, ay=-40, xref="x", yref="y",
                                        text='Verlaag schouw of BIS capaciteit', alignment='left',
                                        showarrow=True, arrowhead=2)],
                   'margin': {'l': 60, 'r': 15, 'b': 40, 't': 40},
                   'plot_bgcolor': colors['plot_bgcolor'],
                   'paper_bgcolor': colors['paper_bgcolor'],
                   }
        }
    record = dict(id='project_performance', figure=fig)
    return record


def set_filters(df: pd.DataFrame):
    filters = [{'label': x, 'value': x} for x in df.project.cat.categories]
    record = dict(filters=filters)
    return record


def get_intersect(a1, a2, b1, b2):
    """
    Returns the point of intersection of the lines passing through a2,a1 and b2,b1.
    a1: [x, y] a point on the first line
    a2: [x, y] another point on the first line
    b1: [x, y] a point on the second line
    b2: [x, y] another point on the second line
    """
    s = np.vstack([a1, a2, b1, b2])  # s for stacked
    h = np.hstack((s, np.ones((4, 1))))  # h for homogeneous
    l1 = np.cross(h[0], h[1])  # get first line
    l2 = np.cross(h[2], h[3])  # get second line
    x, y, z = np.cross(l1, l2)  # point of intersection
    if z == 0:  # lines are parallel
        return (float('inf'), float('inf'))
    return (x / z, y / z)


def info_table(tot_l, d_real_l, HP, y_target_l, x_d, HC_HPend_l, Schouw_BIS, HPend_l, n_err):
    n_w = int((pd.Timestamp.now() - pd.to_datetime('2019-12-30')).days / 7) + 1
    n_d = int((pd.Timestamp.now() - x_d[0]).days)
    n_dw = int((pd.to_datetime('2019-12-30') - x_d[0]).days) + (n_w - 1) * 7
    col = ['project', 'KPN HPend - W' + str(n_w - 1), 'Real HPend - W' + str(n_w - 1), 'Diff - W' + str(n_w - 1),
           'KPN HPend - W' + str(n_w), 'Real HPend - W' + str(n_w), 'Diff - W' + str(n_w), 'HC / HP actueel',
           'Errors FC - BC']
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


def calculate_y_voorraad_act(df: pd.DataFrame):
    # todo add in_has_werkvoorraad column in etl and use that column
    return df[
        (~df.toestemming.isna()) &
        (df.opleverstatus != '0') &
        (df.opleverdatum.isna())
        ].groupby(by="project").count().reset_index()[['project', "sleutel"]].set_index("project").to_dict()['sleutel']


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
    # TODO, remove if in secrets
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


def error_check_FCBC(df: pd.DataFrame):
    business_rules = {}

    no_errors_series = pd.Series([False]).repeat(len(df)).values

    business_rules['101'] = (df.kabelid.isna() & ~df.opleverdatum.isna() & (df.postcode.isna() | df.huisnummer.isna()))
    business_rules['102'] = (df.plandatum.isna())
    business_rules['103'] = (
            df.opleverdatum.isna() & df.opleverstatus.isin(['2', '10', '90', '91', '96', '97', '98', '99']))
    business_rules['104'] = (df.opleverstatus.isna())
    # business_rules['114'] = (df.toestemming.isna())
    business_rules['115'] = business_rules['118'] = (df.soort_bouw.isna())  # soort_bouw hoort bij?
    business_rules['116'] = (df.ftu_type.isna())
    business_rules['117'] = (df['toelichting_status'].isna() & df.opleverstatus.isin(['4', '12']))
    business_rules['119'] = (df['toelichting_status'].isna() & df.redenna.isin(['R8', 'R9', 'R17']))

    business_rules['120'] = no_errors_series  # doorvoerafhankelijk niet aanwezig
    business_rules['121'] = (
                (df.postcode.isna() & ~df.huisnummer.isna()) | (~df.postcode.isna() & df.huisnummer.isna()))
    business_rules['122'] = (
        ~(
                (
                        df.kast.isna() &
                        df.kastrij.isna() &
                        df.odfpos.isna() &
                        df.catvpos.isna() &
                        df.odf.isna()) |
                (
                        ~df.kast.isna() &
                        ~df.kastrij.isna() &
                        ~df.odfpos.isna() &
                        ~df.catvpos.isna() &
                        ~df.areapop.isna() &
                        ~df.odf.isna()
                )
        )
    )  # kloppen deze velden?  (kast, kastrij, odfpos)
    business_rules['123'] = (df.projectcode.isna())
    business_rules['301'] = (~df.opleverdatum.isna() & df.opleverstatus.isin(['0', '14']))
    business_rules['303'] = (df.kabelid.isna() & (df.postcode.isna() | df.huisnummer.isna()))
    business_rules['304'] = no_errors_series  # geen column Kavel...
    business_rules['306'] = (~df.kabelid.isna() & df.opleverstatus.isin(['90', '91', '96', '97', '98', '99']))
    business_rules['308'] = no_errors_series  # geen HLopleverdatum...
    business_rules['309'] = no_errors_series  # geen doorvoerafhankelijk aanwezig...

    business_rules['310'] = no_errors_series  # (~df.KabelID.isna() & df.Areapop.isna())  # strengID != KabelID?
    business_rules['311'] = (df.redenna.isna() & ~df.opleverstatus.isin(['2', '10', '50']))
    business_rules['501'] = ~df.postcode.str.match(r"\d{4}[a-zA-Z]{2}").fillna(False)
    business_rules['502'] = no_errors_series  # niet te checken, geen toegang tot CLR
    business_rules['503'] = no_errors_series  # date is already present in different format...yyyy-mm-dd??
    business_rules['504'] = no_errors_series  # date is already present in different format...yyyy-mm-dd??
    business_rules['506'] = (~df.opleverstatus.isin(
        ['0', '1', '2', '4', '5', '6', '7,' '8', '9', '10', '11', '12', '13', '14', '15', '30', '31', '33', '34', '35',
         '50', '90', '91', '96', '97', '98', '99']))
    business_rules['508'] = no_errors_series  # niet te checken, geen toegang tot Areapop

    def check_numeric_and_lenght(series: pd.Series, min_lenght=1, max_lenght=100, fillna=True):
        return (series.str.len() > max_lenght) | (series.str.len() < min_lenght) | ~(
            series.str.isnumeric().fillna(fillna))

    business_rules['509'] = check_numeric_and_lenght(df.kastrij, max_lenght=2)
    business_rules['510'] = check_numeric_and_lenght(df.kast, max_lenght=4)
    business_rules['511'] = check_numeric_and_lenght(df.odf, max_lenght=5)
    business_rules['512'] = check_numeric_and_lenght(df.odfpos, max_lenght=2)
    business_rules['513'] = check_numeric_and_lenght(df.catv, max_lenght=5)
    business_rules['514'] = check_numeric_and_lenght(df.catvpos, max_lenght=3)

    business_rules['516'] = no_errors_series  # cannot check
    business_rules['517'] = no_errors_series  # date is already present in different format...yyyy-mm-dd??
    business_rules['518'] = (~df.toestemming.isin(['Ja', 'Nee', np.nan]))
    business_rules['519'] = (~df.soort_bouw.isin(['Laag', 'Hoog', 'Duplex', 'Woonboot', 'Onbekend']))
    business_rules['520'] = ((df.ftu_type.isna() & df.opleverstatus.isin(['2', '10'])) | (~df.ftu_type.isin(
        ['FTU_GN01', 'FTU_GN02', 'FTU_PF01', 'FTU_PF02', 'FTU_TY01', 'FTU_ZS_GN01', 'FTU_TK01', 'Onbekend'])))
    business_rules['521'] = (df.toelichting_status.str.len() < 3)
    business_rules['522'] = no_errors_series  # Civieldatum not present in our FC dump
    business_rules['524'] = no_errors_series  # Kavel not present in our FC dump
    business_rules['527'] = no_errors_series  # HL opleverdatum not present in our FC dump
    business_rules['528'] = (~df.redenna.isin(
        [np.nan, 'R0', 'R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'R9', 'R10', 'R11', 'R12', 'R13', 'R14', 'R15',
         'R16', 'R17', 'R18', 'R19', 'R20', 'R21', 'R22']))
    business_rules['531'] = no_errors_series  # strengID niet aanwezig in deze FCdump
    # if df[~df.CATVpos.isin(['999'])].shape[0] > 0:
    #     business_rules['532'] = [df.sleutel[el] for el in df.ODFpos.index
    #                                 if ((int(df.CATVpos[el]) - int(df.ODFpos[el]) != 1) &
    #                                     (int(df.CATVpos[el]) != '999')) |
    #                                    (int(df.ODFpos[el]) % 2 == [])]
    business_rules['533'] = no_errors_series  # Doorvoerafhankelijkheid niet aanwezig in deze FCdump
    business_rules['534'] = no_errors_series  # geen toegang tot CLR om te kunnen checken
    business_rules['535'] = df.toelichting_status.str.contains(",").fillna(False)
    business_rules['536'] = df.kabelid.str.len() < 3
    business_rules['537'] = no_errors_series  # Blok not present in our FC dump
    business_rules['701'] = no_errors_series  # Kan niet gecheckt worden, hebben we vorige waarde voor nodig...
    business_rules['702'] = (~df.odf.isna() & df.opleverstatus.isin(['90', '91', '96', '97', '98', '99']))
    business_rules['707'] = no_errors_series  # Kan niet gecheckt worden, hebben we vorige waarde voor nodig...
    business_rules['708'] = (df.opleverstatus.isin(['90']) & ~df.redenna.isin(['R15', 'R16', 'R17'])) | (
            df.opleverstatus.isin(['91']) & ~df.redenna.isin(['R12', 'R13', 'R14', 'R21']))
    # business_rules['709'] = ((df.ODF + df.ODFpos).duplicated(keep='last'))  # klopt dit?
    business_rules['710'] = ~df.kabelid.isna() & ~df.adres.isna() & (df.kabelid + df.adres).duplicated(keep=False)
    # business_rules['711'] = (~df.CATV.isin(['999']) | ~df.CATVpos.isin(['999']))  # wanneer PoP 999?
    business_rules['713'] = no_errors_series  # type bouw zit niet in onze FC dump
    # if df[df.ODF.isin(['999']) & df.ODFpos.isin(['999']) & df.CATVpos.isin(['999']) & df.CATVpos.isin(['999'])].shape[0] > 0:
    #     business_rules['714'] = df[~df.ODF.isin(['999']) | ~df.ODFpos.isin(['999']) | ~df.CATVpos.isin(['999']) |
    #                                 ~df.CATVpos.isin(['999'])].sleutel.to_list()
    business_rules['716'] = no_errors_series  # niet te checken, geen toegang tot SIMA
    business_rules['717'] = no_errors_series  # type bouw zit niet in onze FC dump
    business_rules['719'] = no_errors_series  # kan alleen gecheckt worden met geschiedenis
    business_rules['721'] = no_errors_series  # niet te checken, geen Doorvoerafhankelijkheid in FC dump
    business_rules['723'] = (df.redenna.isin(['R15', 'R16', 'R17']) & ~df.opleverstatus.isin(['90'])) | (
            df.redenna.isin(['R12', 'R12', 'R14', 'R21']) & ~df.opleverstatus.isin(['91'])) | (
                                    df.opleverstatus.isin(['90']) & df.redenna.isin(['R2', 'R11']))
    business_rules['724'] = (~df.opleverdatum.isna() & df.redenna.isin(['R0', 'R19', 'R22']))
    business_rules['725'] = no_errors_series  # geen zicht op vraagbundelingsproject of niet
    business_rules['726'] = no_errors_series  # niet te checken, geen HLopleverdatum aanwezig
    business_rules['727'] = df.opleverstatus.isin(['50'])
    business_rules['728'] = no_errors_series  # voorkennis nodig over poptype

    business_rules['729'] = no_errors_series  # kan niet checken, vorige staat FC voor nodig
    business_rules['90x'] = no_errors_series  # kan niet checken, extra info over bestand nodig!

    errors_FC_BC = defaultdict(dict)

    for err_no, mask in business_rules.items():
        g_df = df[mask].groupby(by="project")['sleutel'].apply(list)
        for p, sleutels in g_df.items():
            errors_FC_BC[p][err_no] = sleutels

    n_err = {}
    for plaats, err_sleutels in errors_FC_BC.items():
        total_sleutels = set()
        for err, sleutels in err_sleutels.items():
            total_sleutels.update(sleutels)
        n_err[plaats] = len(set(total_sleutels))

    return n_err, errors_FC_BC


def cluster_reden_na(label, clusters):
    for k, v in clusters.items():
        if label in v:
            return k


def pie_chart_reden_na(df_na, clusters, key):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df_na.loc[:, 'cluster_redenna'] = df_na['redenna'].apply(lambda x: cluster_reden_na(x, clusters))
        df_na.loc[df_na['opleverstatus'] == '2', ['cluster_redenna']] = 'HC'
        cluster_types = CategoricalDtype(categories=list(clusters.keys()), ordered=True)
        df_na['cluster_redenna'] = df_na['cluster_redenna'].astype(cluster_types)

        df_na = df_na.groupby('cluster_redenna').size().copy()
        df_na = df_na.to_frame(name='count').reset_index().copy()
        labels = df_na['cluster_redenna'].tolist()
        values = df_na['count'].tolist()

    data = {
        'labels': labels,
        'values': values,
        'marker': {
            'colors':
                [
                    colors['vwt_blue'],
                    colors['yellow'],
                    colors['red'],
                    colors['green']
                ]
        }
    }
    document = 'pie_na_' + key
    return data, document


def overview_reden_na(df: pd.DataFrame, clusters):
    data, document = pie_chart_reden_na(df, clusters, 'overview')
    layout = get_pie_layout()
    fig = dict(data=data, layout=layout)
    record = dict(id=document, figure=fig)
    return record


def individual_reden_na(df: pd.DataFrame, clusters):
    record_dict = {}
    for project, df in df.groupby(by="project"):
        data, document = pie_chart_reden_na(df, clusters, project)
        layout = get_pie_layout()
        fig = {
            'data': data,
            'layout': layout
        }
        fig = dict(data=data, layout=layout)
        record = dict(id=document, figure=fig)
        record_dict[document] = record
    return record_dict


def to_firestore(collection, document, record):
    firestore.Client().collection(collection).document(document).set(record)


def get_pie_layout():
    layout = {
        #   'clickmode': 'event+select',
        'showlegend': True,
        'autosize': True,
        'margin': {'l': 50, 'r': 50, 'b': 100, 't': 100},
        'title': {'text': 'Opgegeven reden na'},
        'height': 500,
        'plot_bgcolor': colors['plot_bgcolor'],
        'paper_bgcolor': colors['paper_bgcolor'],
    }
    return layout


def analyse_documents(date_FTU0, date_FTU1, y_target_l, rc1, x_prog, x_d, d_real_l, df_prog, df_target, df_real,
                      df_plan, HC_HPend, y_prog_l, tot_l, HP, t_shift, rc2, cutoff, y_voorraad_act, HC_HPend_l,
                      Schouw_BIS, HPend_l, n_err, Schouw, BIS):
    for key in y_target_l:
        if (key in date_FTU0) & (key not in date_FTU1):  # estimate target based on average projectspeed
            date_FTU1[key] = x_d[int(round(x_prog[x_d == date_FTU0[key]][0] +
                                           (100 / (sum(rc1.values()) / len(rc1.values())))[0]))].strftime('%Y-%m-%d')
        if (key not in date_FTU0):  # project has finished, estimate target on what has been done
            date_FTU0[key] = x_d[d_real_l[key].index.min()].strftime('%Y-%m-%d')
            date_FTU1[key] = x_d[d_real_l[key].index.max()].strftime('%Y-%m-%d')

    analysis = dict(id='analysis', FTU0=date_FTU0, FTU1=date_FTU1)

    y_prog_l_r = {}
    y_target_l_r = {}
    t_shift_r = {}
    d_real_l_r = {}
    d_real_l_ri = {}
    rc1_r = {}
    rc2_r = {}
    for key in y_prog_l:
        y_prog_l_r[key] = list(y_prog_l[key])
        y_target_l_r[key] = list(y_target_l[key])
        t_shift_r[key] = str(t_shift[key])
        if key in d_real_l:
            d_real_l_r[key] = list(d_real_l[key]['Aantal'])
            d_real_l_ri[key] = list(d_real_l[key].index)
        if key in rc1:
            rc1_r[key] = list(rc1[key])
        if key in rc2:
            rc2_r[key] = list(rc2[key])
    analysis2 = dict(id='analysis2', x_d=[el.strftime('%Y-%m-%d') for el in x_d], tot_l=tot_l, y_prog_l=y_prog_l_r,
                     y_target_l=y_target_l_r, HP=HP, rc1=rc1_r, rc2=rc2_r, t_shift=t_shift_r, cutoff=cutoff,
                     x_prog=[int(el) for el in x_prog], y_voorraad_act=y_voorraad_act, HC_HPend_l=HC_HPend_l,
                     Schouw_BIS=Schouw_BIS, HPend_l=HPend_l)
    analysis3 = dict(id='analysis3', d_real_l=d_real_l_r, d_real_li=d_real_l_ri, n_err=n_err)
    return analysis, analysis2, analysis3


def calculate_redenna_per_period(df: pd.DataFrame, date_column: str = 'hasdatum', freq: str = 'W-MON') -> dict:
    """
    Calculates the number of each reden na cluster (as defined in the config) grouped by
    the date of the 'date_column'. The date is grouped in buckets of the period. For example by week or month.

    Set the freq using: https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
    We commonly use:
        'MS' for the start of the month
        'W-MON' for weeks starting on Monday.

        :param df: The data set
        :param date_column: The column used to group on
        :param freq: The period to use in the grouper
        :return: a dictionary with the first day of the period as key, and the clusters with their occurence counts
                 as value.
    """
    redenna_period_df = df[['cluster_redenna', date_column, 'project']] \
        .groupby(by=[pd.Grouper(key=date_column,
                                freq=freq,
                                closed='left',
                                label="left"
                                ),
                     "cluster_redenna",
                     ]
                 ).count().unstack().fillna(0).project
    redenna_period_df.index = redenna_period_df.index.strftime('%Y-%m-%d')
    return redenna_period_df.to_dict(orient="index")


def rules_to_state(rules_list, state_list):
    """
    This function calculates the state of each row. The provided rules MUST NOT overlap, otherwise there can be no
    unique state determined.

    :param rules_list: A list of masks for a particular datafame.
    :param state_list: The states that the rules describe
    :return: A series of the state for each row in the dataframe.
    """
    if len(rules_list) != len(state_list):
        raise ValueError("The number of rules must be equal to the number of states")
    calculation_df = pd.concat(rules_list, axis=1).astype(int)
    state = calculation_df.apply(
        lambda x: state_list[list(x).index(True)],
        axis=1
    )
    return state
