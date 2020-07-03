import pandas as pd
import numpy as np
from google.cloud import firestore, storage
import os
import time
import json
import datetime
import hashlib


def get_data_from_ingestbucket(gpath_i, col, path_data):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gpath_i
    fn_l = os.listdir(path_data + '../jsonFC/')
    # for fn in fn_l:
    #     os.remove(path_data + '../jsonFC/' + fn)
    client = storage.Client()
    bucket = client.get_bucket('vwt-d-gew1-it-fiberconnect-int-preprocess-stg')
    blobs = bucket.list_blobs()
    for blob in blobs:
        if pd.Timestamp.now().strftime('%Y%m%d') in blob.name:
            # if '20200616' in blob.name:
            # if '20200622' in blob.name:
            blob.download_to_filename(path_data + '../jsonFC/' + blob.name.split('/')[-1])
    fn_l = os.listdir(path_data + '../jsonFC/')

    df_l = {}
    for fn in fn_l:
        df = pd.DataFrame(pd.read_json(path_data + '../jsonFC/' + fn, orient='records')['data'].to_list())
        key = df['title'].iloc[0][0:-13]
        if key not in df_l.keys():
            df_l[key] = df.replace('', np.nan)
        else:
            df_l[key] = df_l[key].append(df.replace('', np.nan))
        if key not in ['Brielle', 'Helvoirt POP Volbouw']:
            os.remove(path_data + '../jsonFC/' + fn)

    for key in df_l:
        df_l[key].rename(columns={'Sleutel': 'sleutel', 'Soort_bouw': 'soort_bouw',
                                  'LaswerkAPGereed': 'laswerkapgereed', 'LaswerkDPGereed': 'laswerkdpgereed',
                                  'Opleverdatum': 'opleverdatum', 'Opleverstatus': 'opleverstatus',
                                  'RedenNA': 'redenna', 'X locatie Rol': 'x_locatie_rol',
                                  'Y locatie Rol': 'y_locatie_rol', 'X locatie DP': 'x_locatie_dp',
                                  'Y locatie DP': 'y_locatie_dp', 'Toestemming': 'toestemming',
                                  'HASdatum': 'hasdatum'}, inplace=True)
        df_l[key]['project'] = df_l[key]['title'].iloc[0][0:-13]
        df_l[key].loc[~df_l[key]['opleverdatum'].isna(), ('opleverdatum')] = \
            [el[6:] + '-' + el[3:5] + '-' + el[0:2] for el in df_l[key][~df_l[key]['opleverdatum'].isna()]['opleverdatum']]
        df_l[key].loc[~df_l[key]['hasdatum'].isna(), ('hasdatum')] = \
            [el[6:] + '-' + el[3:5] + '-' + el[0:2] for el in df_l[key][~df_l[key]['hasdatum'].isna()]['hasdatum']]

    # hash sleutel code
    for key in df_l:
        df_l[key] = df_l[key][~df_l[key].sleutel.isna()]
        df_l[key].sleutel = [hashlib.sha256(el.encode()).hexdigest() for el in df_l[key].sleutel.to_list()]
        df_l[key]['id'] = df_l[key]['project'] + '_' + df_l[key]['sleutel']
        # for i, idx in enumerate(df_l[key][df_l[key]['sleutel'].duplicated()].index):
        #     df_l[key]['sleutel'][idx] = df_l[key]['sleutel'][idx] + '_' + str(i)
        df_l[key] = df_l[key][col]

    return df_l


def get_data_FC(subset, col, gpath_i, path_data):
    if gpath_i is None:
        df_l_r = get_data_projects(col)
    else:
        df_l_r = get_data_from_ingestbucket(gpath_i, col, path_data)

    df_l = {}
    for key in subset:
        if key in df_l_r:
            df_l[key] = df_l_r[key][~df_l_r[key].duplicated()].fillna(np.nan)  # do we take the proper key here???
            # df_l[key] = df_l_r[key][~df_l_r[key].sleutel.duplicated()].fillna(np.nan)  # do we take the proper key here???
            df_l[key].loc[~df_l[key]['opleverdatum'].isna(), ('opleverdatum')] = \
                [el[0:10] for el in df_l[key][~df_l[key]['opleverdatum'].isna()]['opleverdatum']]
            df_l[key].loc[~df_l[key]['hasdatum'].isna(), ('hasdatum')] = \
                [el[0:10] for el in df_l[key][~df_l[key]['hasdatum'].isna()]['hasdatum']]
        else:
            df_l[key] = pd.DataFrame(columns=col)

    t_s = {}
    tot_l = {}
    for key in df_l:
        t_s[key] = pd.to_datetime(df_l[key]['opleverdatum']).min()
        if pd.isnull(t_s[key]):
            t_s[key] = pd.to_datetime(pd.Timestamp.now().strftime('%Y-%m-%d'))
        tot_l[key] = len(df_l[key])  # straks aantal projecten baseren op targets KPN?
        # if key in ['Den Haag', 'Den Haag Regentessekwatier', 'Den Haag Morgenstond west']:
        #     tot_l[key] = 0.3 * tot_l[key]
    x_d = pd.date_range(min(t_s.values()), periods=1000 + 1, freq='D')

    return df_l, t_s, x_d, tot_l


def get_data_planning(path_data, subset_KPN_2020):
    if 'gs://' in path_data:
        xls = pd.ExcelFile(path_data)
    else:
        xls = pd.ExcelFile(path_data + 'Data_20200101_extra/Forecast JUNI 2020_def.xlsx')
    df = pd.read_excel(xls, 'FTTX ').fillna(0)
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


def get_data_targets(path_data):
    if path_data is None:
        doc = next(firestore.Client().collection('Graphs').where('id', '==', 'analysis').get()).to_dict()
        date_FTU0 = doc['FTU0']
        date_FTU1 = doc['FTU1']
    else:
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
            'Bergen op Zoom Noord  wijk 01 + Halsteren': 'Bergen op Zoom Noord  wijk 01 + Halsteren',  # niet in FC
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
            d_real[d_real.Aantal > 100] = 100  # alleen nodig voor DH
            t_shift[key] = (d_real.index.min() - min(t_s.values())).days
            d_real.index = (d_real.index - d_real.index[0]).days + t_shift[key]
            d_real_l[key] = d_real

            d_rc1 = d_real[d_real.Aantal < cutoff]
            if len(d_rc1) > 1:
                rc1[key], b1 = np.polyfit(d_rc1.index, d_rc1, 1)
                y_prog1 = b1[0] + rc1[key][0] * x_prog
                y_prog_l[key] = y_prog1.copy()

            d_rc2 = d_real[d_real.Aantal >= cutoff]
            if len(d_rc2) > 1:
                rc2[key], b2 = np.polyfit(d_rc2.index, d_rc2, 1)
                y_prog2 = b2[0] + rc2[key][0] * x_prog
                x_i, y_i = get_intersect([x_prog[0], y_prog1[0]], [x_prog[-1], y_prog1[-1]],
                                         [x_prog[0], y_prog2[0]], [x_prog[-1], y_prog2[-1]])
                y_prog_l[key][x_prog >= x_i] = y_prog2[x_prog >= x_i]

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


def calculate_projectspecs(df_l):
    # to calculate HC / HPend ratio:
    HC = {}
    HPend = {}
    HC_HPend_l = {}
    Schouw_BIS = {}
    HPend_l = {}
    for key in df_l:
        HC[key] = len(df_l[key][df_l[key].opleverstatus == '2'])
        HPend[key] = len(df_l[key][~df_l[key].opleverdatum.isna()])
        opgeleverd = len(df_l[key][~df_l[key].opleverdatum.isna()])
        if opgeleverd > 0:
            HC_HPend_l[key] = len(df_l[key][df_l[key].opleverstatus == '2']) / opgeleverd * 100
        else:
            HC_HPend_l[key] = 0
        Schouw_BIS[key] = len(df_l[key][(~df_l[key].toestemming.isna()) & (df_l[key].opleverstatus != '0')])
        HPend_l[key] = len(df_l[key][~df_l[key].opleverdatum.isna()])
    HC_HPend = round(sum(HC.values()) / sum(HPend.values()), 2)

    return HC_HPend, HC_HPend_l, Schouw_BIS, HPend_l


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


def graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, res):
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
                             HC_HPend=str(HC_HPend), prog_c='pretty_container')
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
    if 'M' == res:
        firestore.Client().collection('Graphs').document('jaaroverzicht').set(jaaroverzicht)
        record = dict(id='graph_targets_M', figure=fig)
    firestore.Client().collection('Graphs').document(record['id']).set(record)


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
        firestore.Client().collection('Graphs').document(record['id']).set(record)


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


def masks_phases(df_l):
    for jj, pkey in enumerate(df_l):
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
        print('23')
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
                    print(ii)
                    batch.commit()
                    batch = firestore.Client().batch()
        batch.commit()
        print(str(jj + 1) + '/' + str(len(df_l)))
    record = dict(id='bar_names', bar_names=bar_names)
    firestore.Client().collection('Graphs').document(record['id']).set(record)


def consume(df_l):
    t = time.time()
    for key_p in df_l:  # niet nodig in gcp
        df = df_l[key_p]  # niet nodig in gcp
        records = df.to_dict('records')  # niet nodig in gcp
        batch = firestore.Client().batch()
        for i, row in enumerate(records):
            record = row
            # record['id'] = row['title'][0:-13] + '_' + row['sleutel']
            # record['index'] = i  # niet nodig in gcp
            # record['project'] = row['title'][0:-13]
            batch.set(firestore.Client().collection('Projects').document(record['id']), record)
            if (i + 1) % 500 == 0:
                batch.commit()
        batch.commit()
        print(key_p + ' ' + str(i+1))
        print('Time: ' + str((time.time() - t)/60) + ' minutes')


def get_data_projects(col):
    doc = next(firestore.Client().collection('Graphs').where('id', '==', 'pnames').get()).to_dict()['filters']
    pnames = []
    for item in doc:
        pnames += [item['label']]
    t = time.time()
    df_l = {}
    for key_p in pnames:
        docs = firestore.Client().collection('Projects').where('project', '==', key_p).stream()
        records = []
        for doc in docs:
            records += [doc.to_dict()]
        if records != []:
            df_l[key_p] = pd.DataFrame(records)[col]
        else:
            df_l[key_p] = pd.DataFrame(columns=col)
        print(key_p)
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
                      'xaxis': {'title': '(HPend gerealiseerd - Target KPN) /  HPend totaal [%]', 'range': [x_min, x_max],
                                'zeroline': False},
                      'yaxis': {'title': '(Geschouwd + BIS) / werkvoorraad [%]', 'range': [y_min, y_max], 'zeroline': False},
                      'showlegend': False,
                      'title': {'text': 'Krijg alle projecten in het groene vlak doormiddel van de pijlen te volgen'},
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
    firestore.Client().collection('Graphs').document(record['id']).set(record)


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


def info_table(tot_l, d_real_l, HP, y_target_l, x_d, HC_HPend_l, Schouw_BIS, HPend_l):
    n_now = int((pd.Timestamp.now() - pd.to_datetime('2019-12-30')).days / 7) + 1
    col = ['project', 'real vs KPN', 'real vs plan', 'HC / HP', 'Schouw & BIS gereed', 'HPend', 'woningen']
    records = []
    for key in d_real_l:
        if d_real_l[key].max()[0] < 100:
            record = dict(project=key)
            record['real vs KPN'] = round((d_real_l[key].max() - y_target_l[key][int((pd.Timestamp.now() -
                                          x_d[0]).days)]) / 100 * tot_l[key])[0]
            record['HC / HP'] = round(HC_HPend_l[key])
            if key in HP.keys():
                record['real vs plan'] = round(d_real_l[key].max() / 100 * tot_l[key] - sum(HP[key][:n_now]))[0]
            else:
                record['real vs plan'] = 0
            record['Schouw & BIS gereed'] = round(Schouw_BIS[key])
            record['HPend'] = round(HPend_l[key])
            # record['HAS gepland'] = round(len(df_l[key][~df_l[key].opleverdatum.isna()]))
            record['woningen'] = round(tot_l[key])
            records += [record]
    df_table = pd.DataFrame(records).to_json(orient='records')
    firestore.Client().collection('Graphs').document('info_table').set(dict(id='info_table', table=df_table, col=col))


def analyse_to_firestore(date_FTU0, date_FTU1, y_target_l, rc1, x_prog, x_d, d_real_l, df_prog, df_target, df_real,
                         df_plan, HC_HPend, y_prog_l, tot_l, HP, t_shift, rc2, cutoff, y_voorraad_act, HC_HPend_l, Schouw_BIS, HPend_l):
    for key in y_target_l:
        if (key in date_FTU0) & (key not in date_FTU1):  # estimate target based on average projectspeed
            date_FTU1[key] = x_d[int(round(x_prog[x_d == date_FTU0[key]][0] +
                                           (100 / (sum(rc1.values()) / len(rc1.values())))[0]))].strftime('%Y-%m-%d')
        if (key not in date_FTU0):  # project has finished, estimate target on what has been done
            date_FTU0[key] = x_d[d_real_l[key].index.min()].strftime('%Y-%m-%d')
            date_FTU1[key] = x_d[d_real_l[key].index.max()].strftime('%Y-%m-%d')

    record = dict(id='analysis', FTU0=date_FTU0, FTU1=date_FTU1)
    firestore.Client().collection('Graphs').document(record['id']).set(record)

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
    record = dict(id='analysis2', x_d=[el.strftime('%Y-%m-%d') for el in x_d], tot_l=tot_l, y_prog_l=y_prog_l_r,
                  y_target_l=y_target_l_r, HP=HP, rc1=rc1_r, rc2=rc2_r, t_shift=t_shift_r, cutoff=cutoff,
                  x_prog=[int(el) for el in x_prog], y_voorraad_act=y_voorraad_act, HC_HPend_l=HC_HPend_l,
                  Schouw_BIS=Schouw_BIS, HPend_l=HPend_l)
    firestore.Client().collection('Graphs').document(record['id']).set(record)
    record = dict(id='analysis3', d_real_l=d_real_l_r, d_real_li=d_real_l_ri)
    firestore.Client().collection('Graphs').document(record['id']).set(record)


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
