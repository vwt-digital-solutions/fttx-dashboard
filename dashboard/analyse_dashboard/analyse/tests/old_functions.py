import pandas as pd
import numpy as np
from functions import get_hc_hpend_ratio_total, get_intersect


def get_start_time_old(df_l):
    # What does t_s stand for? Would prefer to use a descriptive variable name.
    t_s = {}
    for key in df_l:
        if df_l[key][~df_l[key].opleverdatum.isna()].empty:
            t_s[key] = pd.to_datetime(pd.Timestamp.now().strftime('%Y-%m-%d'))
        else:  # I'm not sure its desireable to hard-set dates like this. Might lead to unexpected behaviour.
            t_s[key] = pd.to_datetime(df_l[key]['opleverdatum']).min()
    return t_s


def get_total_objects_old(df_l):  # Don't think this is necessary to calculate at this point, should be done later.
    total_objects = {k: len(v) for k, v in df_l.items()}
    # This hardcoded stuff can lead to unexpected behaviour. Should this still be in here?
    total_objects['Bergen op Zoom Noord  wijk 01 + Halsteren'] = 9.465  # not yet in FC, total from excel bouwstromen
    total_objects['Den Haag - Haagse Hout-Bezuidenhout West'] = 9.488  # not yet in FC, total from excel bouwstromen
    total_objects['Den Haag - Vrederust en Bouwlust'] = 11.918  # not yet in FC, total from excel bouwstromen
    return total_objects


def add_relevant_columns_old(df_l, year):
    def object_is_hpend(opleverdatum, year):
        # Will return TypeError if opleverdatum is nan, in which case object is not hpend
        try:
            is_hpend = (opleverdatum >= year + '-01-01') & (opleverdatum <= year + '-12-31')
        except TypeError:
            is_hpend = False
        return is_hpend

    for k, v in df_l.items():
        v['hpend'] = v.opleverdatum.apply(lambda x: object_is_hpend(x, '2020'))
        v['homes_completed'] = v.opleverstatus == '2'
        v['bis_gereed'] = v.opleverstatus != '0'
    return df_l


def get_homes_completed_old(df_l):
    return {k: sum(v.homes_completed) for k, v in df_l.items()}


def get_HPend_old(df_l):
    return {k: sum(v.hpend) for k, v in df_l.items()}


def get_has_ready_old(df_l):
    return {k: len(v[~v.toestemming.isna() & v.bis_gereed]) for k, v in df_l.items()}


def calculate_y_voorraad_act_old(df_l):
    y_voorraad_act = {}
    for key in df_l:
        y_voorraad_act[key] = len(df_l[key][(~df_l[key].toestemming.isna()) &
                                            (df_l[key].opleverstatus != '0') &
                                            (df_l[key].opleverdatum.isna())])

    return y_voorraad_act


def get_has_werkvoorraad_old(df_l):
    return sum(calculate_y_voorraad_act_old(df_l).values())


def get_hc_hpend_ratio_old(df_l):
    ratio_per_project = {}
    for project, data in df_l.items():
        try:
            ratio_per_project[project] = sum(data.homes_completed) / sum(data.hpend) * 100
        except ZeroDivisionError:
            # Dirty fix, check if it can be removed.
            ratio_per_project[project] = 0
    return ratio_per_project


def preprocess_data_old(df_l, year):
    df_l = add_relevant_columns_old(df_l, year)
    return df_l


def calculate_projectspecs_old(df_l):
    homes_completed = get_homes_completed_old(df_l)
    homes_ended = get_HPend_old(df_l)
    has_ready = get_has_ready_old(df_l)
    hc_hpend_ratio = get_hc_hpend_ratio_old(df_l)
    hc_hp_end_ratio_total = get_hc_hpend_ratio_total(homes_completed, homes_ended)
    werkvoorraad = get_has_werkvoorraad_old(df_l)

    return hc_hp_end_ratio_total, hc_hpend_ratio, has_ready, homes_ended, werkvoorraad


def prognose_old(df_l, t_s, x_d, tot_l, date_FTU0):
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


def set_filters_old(df_l):
    filters = []
    for key in df_l:
        filters += [{'label': key, 'value': key}]
    record = dict(filters=filters)
    return record
