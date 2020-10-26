from collections import defaultdict
from typing import NamedTuple

import pandas as pd
import numpy as np
from google.cloud import firestore
import time
import datetime

import business_rules as br
import config
from collections import namedtuple

colors = config.colors_vwt


# Function to use only when data_targets in database need to be reset.
# TODO: Create function structure that can reinitialise the database, partially as well as completely.
def get_data_targets_init(path_data, map_key):
    df_targets = pd.read_excel(path_data, sheet_name='KPN')
    date_FTU0 = {}
    date_FTU1 = {}
    for i, key in enumerate(df_targets['d.d. 01-05-2020 v11']):
        if key in map_key:
            if not pd.isnull(df_targets.loc[i, '1e FTU']):
                date_FTU0[map_key[key]] = df_targets.loc[i, '1e FTU'].strftime('%Y-%m-%d')
            if (not pd.isnull(df_targets.loc[i, 'Laatste FTU'])) & (df_targets.loc[i, 'Laatste FTU'] != '?'):
                date_FTU1[map_key[key]] = df_targets.loc[i, 'Laatste FTU'].strftime('%Y-%m-%d')

    return date_FTU0, date_FTU1


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
    tmp_df['has_ready'] = br.has_werkvoorraad(tmp_df)
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
    temp_df['has_opleverdatum'] = br.opgeleverd(df)
    sum_df = temp_df[['sleutel', "project", "has_opleverdatum", 'homes_completed_total']].groupby(
        by="project").sum().reset_index()
    sum_df['ratio'] = sum_df.apply(
        lambda x: x.homes_completed_total / x.has_opleverdatum * 100
        if x.has_opleverdatum else 0, axis=1
    )
    return sum_df[['project', 'ratio']].set_index("project").to_dict()['ratio']


def get_has_werkvoorraad(df: pd.DataFrame):
    # todo add in_has_werkvoorraad column in etl and use that column
    return calculate_ready_for_has(df)


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


def get_cumsum_of_col(df: pd.DataFrame, column):
    # Can we make it generic by passing column, or does df need to be filtered beforehand?
    filtered_df = df[~df[column].isna()]

    # Maybe we can move the rename of this column to preprocessing?
    agg_df = filtered_df.groupby([column]).agg({'sleutel': 'count'}).rename(columns={'sleutel': 'Aantal'})
    cumulative_df = agg_df.cumsum()

    return cumulative_df


def get_real_df(df: pd.DataFrame, t_s, tot_l):
    d_real_l = {}
    for project, project_df in df.groupby(by="project"):

        project_df_real = project_df[~project_df['opleverdatum'].isna()]  # todo opgeleverd gebruken?
        project_df_real_counts = project_df_real.groupby(['opleverdatum']).agg({'sleutel': 'count'}).rename(columns={'sleutel': 'Aantal'})
        project_df_real_counts.index = pd.to_datetime(project_df_real_counts.index, format='%Y-%m-%d')
        project_df_real_counts = project_df_real_counts.sort_index()

        project_df_realised_counts_to_present = project_df_real_counts[project_df_real_counts.index < pd.Timestamp.now()]
        project_df_realised_counts_to_present_percentage = project_df_realised_counts_to_present.cumsum() / tot_l[project] * 100

        # first date in counts dataframe
        min_date = project_df_realised_counts_to_present_percentage.index.min()

        # What does this date represent? Should this be in business rules?
        min_shift = min(t_s.values())

        # Amount of days this specific projects starts after the start of the 'earliest' project?
        t_shift = get_t_shift(min_date, min_shift)

        # Dirty fix, still necessary?
        # only necessary for DH
        project_df_realised_counts_to_present_percentage[project_df_realised_counts_to_present_percentage.Aantal > 100] = 100

        d_real = 'dummy'
        # I think I'd prefer messing with the index completely separately.
        d_real.index = (d_real.index - d_real.index[0]).days + t_shift[project]
        d_real_l[project] = d_real
    return d_real_l


def get_t_shift(min_date, min_shift):
    return (min_date - min_shift).days


def prognose(df: pd.DataFrame, t_s, x_d, tot_l, date_FTU0):
    x_prog = np.array(list(range(0, len(x_d))))
    cutoff = 85

    rc1 = {}
    rc2 = {}
    d_real_l = {}
    t_shift = {}
    y_prog_l = {}
    for project, project_df in df.groupby(by="project"):  # to calculate prognoses for projects in FC
        d_real = project_df[~project_df['opleverdatum'].isna()]  # todo opgeleverd gebruken?
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
    if rc2:
        rc2_mean = sum(rc2.values()) / len(rc2.values())
    else:
        rc2_mean = 0.5 * rc1_mean  # temp assumption that after cutoff value effectivity of has process decreases by 50%

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


def fill_2020(df):
    filler2020 = pd.DataFrame(index=pd.date_range(start='2020-01-01', end=df.index[0], freq='D'),
                              columns=['d'],
                              data=0)
    df = pd.concat([filler2020[0:-1], df], axis=0)
    return df


def percentage_to_amount(percentage, total):
    return percentage / 100 * total


def transform_to_amounts(percentage_dict, total_dict, days_index):
    df = pd.DataFrame(index=days_index, columns=['d'], data=0)
    for key in percentage_dict:
        amounts = percentage_to_amount(percentage_dict[key], total_dict[key])
        df += pd.DataFrame(index=days_index, columns=['d'], data=amounts).diff().fillna(0)
    if df.index[0] > pd.Timestamp('2020-01-01'):
        df = fill_2020(df)
    return df


def transform_df_real(percentage_dict, total_dict, days_index):
    df = pd.DataFrame(index=days_index, columns=['d'], data=0)
    for key in percentage_dict:
        y_real = (percentage_dict[key] / 100 * total_dict[key]).diff().fillna((percentage_dict[key] / 100 * total_dict[key]).iloc[0])
        y_real = y_real.rename(columns={'Aantal': 'd'})
        y_real.index = days_index[y_real.index]
        df = df.add(y_real, fill_value=0)
    if df.index[0] > pd.Timestamp('2020-01-01'):
        df = fill_2020(df)
    return df


def transform_df_plan(x_d, HP):
    df = pd.DataFrame(index=x_d, columns=['d'], data=0)
    y_plan = pd.DataFrame(index=pd.date_range(start='30-12-2019', periods=len(HP['HPendT']), freq='W-MON'),
                          columns=['d'], data=HP['HPendT'])
    y_plan = y_plan.cumsum().resample('D').mean().interpolate().diff().fillna(y_plan.iloc[0])
    df = df.add(y_plan, fill_value=0)
    if df.index[0] > pd.Timestamp('2020-01-01'):
        df = fill_2020(df)
    return df


def overview(x_d, y_prog_l, tot_l, d_real_l, HP, y_target_l):
    df_prog = transform_to_amounts(y_prog_l, tot_l, x_d)
    df_target = transform_to_amounts(y_target_l, tot_l, x_d)
    df_real = transform_df_real(d_real_l, tot_l, x_d)
    df_plan = transform_df_plan(x_d, HP)
    OverviewResults = namedtuple("OverviewResults", ['df_prog', 'df_target', 'df_real', 'df_plan'])
    return OverviewResults(df_prog, df_target, df_real, df_plan)


def graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad, res, show_planning=True):
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

    if not show_planning:
        data_p = dict.fromkeys(data_p, 0)

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

    target_sum = str(round(sum(target[1:])))
    planning_sum = sum(planning[n_now:]) - realisatie[n_now]
    prognose_sum = sum(prognose[n_now:]) - realisatie[n_now]
    realisatie_sum = str(round(sum(realisatie[1:])))

    jaaroverzicht = dict(id='jaaroverzicht',
                         target=str(int(target_sum)),
                         real=str(int(realisatie_sum)),
                         plan=str(int(planning_sum)),
                         prog=str(int(prognose_sum)),
                         HC_HPend=str(HC_HPend),
                         HAS_werkvoorraad=str(int(HAS_werkvoorraad)),
                         prog_c='pretty_container')
    if jaaroverzicht['prog'] < jaaroverzicht['plan']:
        jaaroverzicht['prog_c'] = 'pretty_container_red'
    return jaaroverzicht


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


def days_in_2019(timeline):
    return len(timeline[timeline < pd.to_datetime('2020-01-01')])


def calculate_weektarget(project, y_target_l, total_objects, timeline):  # berekent voor de week t/m de huidige dag
    index_firstdaythisweek = days_in_2019(timeline) + pd.Timestamp.now().dayofyear - pd.Timestamp.now().dayofweek - 1
    if project in y_target_l:
        value_atstartweek = y_target_l[project][index_firstdaythisweek - 1]
        value_atendweek = y_target_l[project][index_firstdaythisweek + 7]
        target = int(round((value_atendweek - value_atstartweek) / 100 * total_objects[project]))
    else:
        target = 0
    return dict(counts=target, counts_prev=None, title='Target week ' + str(pd.Timestamp.now().week),
                subtitle='', font_color='green', id=None)


def calculate_weekrealisatie(project, d_real_l, total_objects, timeline,
                             delay):  # berekent voor de week t/m de huidige dag
    index_firstdaythisweek = days_in_2019(timeline) + pd.Timestamp.now().dayofyear - pd.Timestamp.now().dayofweek - 1
    if project in d_real_l:
        value_atstartweek = d_real_l[project][d_real_l[project].index <= index_firstdaythisweek - 1 + delay * 7][
            'Aantal'].max()
        value_atendweek = d_real_l[project][d_real_l[project].index <= index_firstdaythisweek + 7 + delay * 7][
            'Aantal'].max()
        # value_atstartweek_min1W = d_real_l[project][
        #   d_real_l[project].index <= index_firstdaythisweek - 1 - 7 + delay * 7]['Aantal'].max()
        # value_atendweek_min1W = d_real_l[project][
        #   d_real_l[project].index <= index_firstdaythisweek + 7 - 7 + delay * 7]['Aantal'].max()
        realisatie = int(round((value_atendweek - value_atstartweek) / 100 * total_objects[project]))
        # realisatie_min1W = int(round((value_atendweek_min1W - value_atstartweek_min1W) / 100 * total_objects[project]))
    else:
        realisatie = 0
        # realisatie_min1W = 0
    return dict(counts=realisatie, counts_prev=None,
                title='Realisatie week ' + str(pd.Timestamp.now().week + delay), subtitle='', font_color='green',
                id=None)


def calculate_weekdelta(project, y_target_l, d_real_l, total_objects,
                        timeline):  # berekent voor de week t/m de huidige dag
    target = calculate_weektarget(project, y_target_l, total_objects, timeline)['counts']
    record = calculate_weekrealisatie(project, d_real_l, total_objects, timeline, delay=0)
    delta = record['counts'] - target
    # delta_min1W = record['counts_prev'] - target
    return dict(counts=delta, counts_prev=None, title='Delta', subtitle='', font_color='green', id=None)


def calculate_weekHCHPend(project, HC_HPend_l):
    return dict(counts=round(HC_HPend_l[project]) / 100, counts_prev=None, title='HC / HPend', subtitle='',
                font_color='green', id=None)


def calculate_weeknerr(project, n_err):
    return dict(counts=n_err[project], counts_prev=None, title='Errors FC- BC', subtitle='', font_color='green',
                id=None)


def calculate_y_voorraad_act(df: pd.DataFrame):
    # todo add in_has_werkvoorraad column in etl and use that column
    return df[br.has_werkvoorraad(df)] \
        .groupby(by="project").count().reset_index()[['project', "sleutel"]].set_index("project").to_dict()['sleutel']


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


def set_date_update():
    record = dict(id='update_date', date=pd.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))
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
    counts = df_na[['cluster_redenna', 'sleutel']].groupby("cluster_redenna").count().reset_index()
    labels = counts.cluster_redenna.to_list()
    values = counts.sleutel.to_list()

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
    document = f"pie_na_{key}"
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
        record = dict(id=document, data=data)
        record_dict[document] = record
    return record_dict


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


def get_project_dates(date_FTU0, date_FTU1, y_target_l, x_prog, x_d, rc1, d_real_l):
    for key in y_target_l:
        if (key in date_FTU0) & (key not in date_FTU1):  # estimate target based on average projectspeed
            date_FTU1[key] = x_d[int(round(x_prog[x_d == date_FTU0[key]][0] +
                                           (100 / (sum(rc1.values()) / len(rc1.values())))[0]))].strftime('%Y-%m-%d')
        if (key not in date_FTU0):  # project has finished, estimate target on what has been done
            date_FTU0[key] = x_d[d_real_l[key].index.min()].strftime('%Y-%m-%d')
            date_FTU1[key] = x_d[d_real_l[key].index.max()].strftime('%Y-%m-%d')

    analysis = dict(FTU0=date_FTU0, FTU1=date_FTU1)
    return analysis


def analyse_documents(y_target_l, rc1, x_prog, x_d, d_real_l, df_prog, df_target, df_real,
                      df_plan, HC_HPend, y_prog_l, tot_l, HP, t_shift, rc2, cutoff, y_voorraad_act, HC_HPend_l,
                      Schouw_BIS, HPend_l, n_err, Schouw, BIS):
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
    return analysis2, analysis3


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


def wait_bins(df: pd.DataFrame, time_delta_days: int = 0) -> pd.DataFrame:
    """
    This function counts the wait between toestemming datum and now (or a reference date, based on time_delta_days).
    It only considers houses which are not connected yet.
    :param df:
    :param time_delta_days:
    :return:
    """
    time_point = (pd.Timestamp.today() - pd.Timedelta(days=time_delta_days))
    mask = ~br.opgeleverd(df, time_delta_days) & br.toestemming_bekend(df)
    toestemming_df = df[mask][['toestemming', 'toestemming_datum', 'opleverdatum', 'cluster_redenna']]

    toestemming_df['waiting_time'] = (time_point - toestemming_df.toestemming_datum).dt.days / 7
    toestemming_df['bins'] = pd.cut(toestemming_df.waiting_time,
                                    bins=[-np.inf, 0, 8, 12, np.inf],
                                    labels=['before_order', 'on_time', 'limited_time', 'late'])
    return toestemming_df


def count_toestemming(toestemming_df):
    toestemming_df = toestemming_df.rename(columns={'bins': "counts"})
    counts = toestemming_df.counts.value_counts()
    return counts


def wait_bin_cluster_redenna(toestemming_df):
    wait_bin_cluster_redenna_df = toestemming_df[['bins', 'cluster_redenna', 'toestemming']].groupby(
        by=['bins', 'cluster_redenna']).count()
    wait_bin_cluster_redenna_df = wait_bin_cluster_redenna_df.rename(columns={"toestemming": "count"})
    wait_bin_cluster_redenna_df = wait_bin_cluster_redenna_df.fillna(value={'count': 0})
    return wait_bin_cluster_redenna_df


def calculate_ready_for_has(df, time_delta_days=0):
    schouw_df = df[['schouwdatum', 'opleverdatum', 'toestemming', 'toestemming_datum', 'opleverstatus']]

    ready_for_has_df = schouw_df[br.has_werkvoorraad(schouw_df, time_delta_days)]
    return len(ready_for_has_df)


def calculate_ready_for_has_indicator(project_df):
    count_now = calculate_ready_for_has(project_df)
    count_prev = calculate_ready_for_has(project_df, time_delta_days=7)
    return {'ready_for_has': {"counts": count_now, "counts_prev": count_prev}}


def calculate_wait_indicators(project_df):
    toestemming_df = wait_bins(project_df)
    toestemming_df_prev = wait_bins(project_df, time_delta_days=7)

    counts = count_toestemming(toestemming_df)
    counts_prev = count_toestemming(toestemming_df_prev)

    counts_df = pd.DataFrame(counts).join(pd.DataFrame(counts_prev), rsuffix="_prev")
    result_dict = counts_df.to_dict(orient='index')
    wait_bin_cluster_redenna_df = wait_bin_cluster_redenna(toestemming_df)
    for index, grouped_df in wait_bin_cluster_redenna_df.groupby('bins'):
        result_dict[index]['cluster_redenna'] = \
            grouped_df.reset_index(level=0, drop=True).to_dict(orient='dict')['count']
    return result_dict


def calculate_projectindicators_tmobile(df: pd.DataFrame):
    markup_dict = {
        'on_time': {'title': 'Openstaande orders op tijd',
                    'subtitle': '< 8 weken',
                    'font_color': 'green'},
        'limited_time': {'title': 'Openstaande orders nog beperkte tijd',
                         'subtitle': '> 8 weken < 12 weken',
                         'font_color': 'orange'},
        'late': {'title': 'Openstaande orders te laat',
                 'subtitle': '> 12 weken',
                 'font_color': 'red'},
        'ratio': {'title': 'Ratio op tijd gesloten orders',
                  'subtitle': '<8 weken',
                  'font_color': 'black'},
        'before_order': {'title': '', 'subtitle': '', 'font_color': ''},
        'ready_for_has': {
            'title': "Werkvoorraad HAS",
        }
    }

    counts_by_project = {}
    for project, project_df in df.groupby(by='project'):
        counts_by_project[project] = {}

        counts_by_project[project].update(
            calculate_ready_for_has_indicator(project_df=project_df)
        )
        counts_by_project[project].update(
            calculate_wait_indicators(project_df=project_df)
        )
        counts_by_project[project].update(
            {'ratio': {'counts': calculate_on_time_ratio(project_df)}}
        )
        for indicator, markup in markup_dict.items():
            counts_by_project[project][indicator].update(markup)
    return counts_by_project


def calculate_on_time_ratio(df):
    # Maximum days an order is allowed to take in days
    max_order_time = 56
    ordered = df[df.ordered & df.opgeleverd]
    on_time = ordered[ordered.oplevertijd <= max_order_time]
    on_time_ratio = len(on_time) / len(ordered)
    return on_time_ratio


def calculate_oplevertijd(row):
    # Do not calculate an oplevertijd if row was not ordered or not opgeleverd
    if row.ordered and row.opgeleverd:
        oplevertijd = (row.opleverdatum - row.toestemming_datum).days
    else:
        oplevertijd = np.nan
    return oplevertijd
