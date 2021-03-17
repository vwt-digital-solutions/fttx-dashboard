"""
functions.py
================

A big collection of functions used in the analysis.
"""

from collections import defaultdict

import pandas as pd
import numpy as np
from google.cloud import firestore, secretmanager
import time
from dateutil.relativedelta import relativedelta
import datetime
import math

from sqlalchemy import create_engine

import business_rules as br
import config
from collections import namedtuple

colors = config.colors_vwt


def get_start_time(df: pd.DataFrame) -> dict:
    """
    The start time of a project is determined by the first `opleverdatum`, therefore an opleverdatum column must be
    available. The start time of a project is the minimal opleverdatum.

    Args:
        df (pd.DataFrame): A dataframe containing a column `opleverdatum` with dates.

    Returns:
        dict: A dictionary with the projects for keys and the start time for each project as the value.
    """
    start_time_by_project = {}
    for project, project_df in df.groupby("project"):
        start_time = project_df.opleverdatum.min()
        if start_time is pd.NaT:
            start_time_by_project[project] = pd.to_datetime(pd.Timestamp.now().strftime('%Y-%m-%d'))
        else:
            start_time_by_project[project] = start_time
    return start_time_by_project


def get_timeline(time_sequence) -> pd.DatetimeIndex:
    """
    Make a date_range to be used as x-axis from the start of a given sequence of date values.
    Args:
        time_sequence: a dictionary of datetime values.

    Returns:a daterange, ranging from input to 2000 days after input date.

    """
    x_axis = pd.date_range(min(time_sequence.values()), periods=2000 + 1, freq='D')
    return x_axis


# TODO: Documentation by Andre van Turnhout
def get_total_objects(df):  # Don't think this is necessary to calculate at this point, should be done later.
    total_objects = df[['sleutel', 'project']].groupby(by="project").count().to_dict()['sleutel']
    # This hardcoded stuff can lead to unexpected behaviour. Should this still be in here?
    # total_objects['Bergen op Zoom Noord Halsteren'] = 9465  # not yet in FC, total from excel bouwstromen
    # total_objects['Den Haag Bezuidenhout'] = 9488  # not yet in FC, total from excel bouwstromen
    # total_objects['Den Haag Vredelust Bouwlust'] = 11918  # not yet in FC, total from excel bouwstromen
    return total_objects


# # TODO: Documentation by Casper van Houten
# def targets(x_prog, x_d, t_shift, date_FTU0, date_FTU1, rc1, d_real_l, total_objects):
#     # to add target info KPN in days uitgaande van FTU0 en FTU1
#     y_target_l = {}
#     t_diff = {}
#     target_per_week_dict = {}
#     for key in t_shift:
#         if (key in date_FTU0) & (key in date_FTU1):
#             t_start = x_prog[x_d == date_FTU0[key]][0]
#             t_max = x_prog[x_d == date_FTU1[key]][0]
#             t_diff[key] = t_max - t_start - 14  # two weeks round up
#             richtings_coefficient = 100 / t_diff[key]  # target naar KPN is 100% HPend
#         if (key in date_FTU0) & (key not in date_FTU1):  # estimate target based on average projectspeed
#             t_start = x_prog[x_d == date_FTU0[key]][0]
#             t_diff[key] = (100 / (sum(rc1.values()) / len(rc1.values())) - 14)[0]  # two weeks round up
#             richtings_coefficient = 100 / t_diff[key]  # target naar KPN is 100% HPend
#         if (key not in date_FTU0):  # project has finished, estimate target on what has been done
#             t_start = d_real_l[key].index.min()
#             t_max = d_real_l[key].index.max()
#             t_diff[key] = t_max - t_start - 14  # two weeks round up
#             richtings_coefficient = 100 / t_diff[key]  # target naar KPN is 100% HPend
#
#         b = -(richtings_coefficient * (t_start + 14))  # two weeks startup
#         y_target = b + richtings_coefficient * x_prog
#         y_target[y_target > 100] = 100
#         y_target_l[key] = y_target
#
#         # richtings_coefficient is the percentage of homes to be completed in a day (basically the target per day)
#         target_per_week_dict[key] = richtings_coefficient / 100 * total_objects[key] * 7
#
#     for key in y_target_l:
#         y_target_l[key][y_target_l[key] > 100] = 100
#         y_target_l[key][y_target_l[key] < 0] = 0
#
#     TargetResults = namedtuple("TargetResults", ['y_target_l', 't_diff', "target_per_week_dict"])
#     return TargetResults(y_target_l, t_diff, target_per_week_dict)


# TODO: Documentation by Andre van Turnhout. How does it calculate the target?
def targets_new(x_d, list_of_projects, date_FTU0, date_FTU1, total_objects):
    """

    This function calculates the target

    Args:
        x_d: datetimeindex (timeline) where all projects can be mapped on
        list_of_projects: projects to loop over
        date_FTU0: FTU0 date per project
        date_FTU1: FTU1 date per project
        total_objects: total objects per project

    Returns:
        target information for KPN per weeks

    """
    # to add target info KPN in days uitgaande van FTU0 en FTU1
    x_prog = np.array(list(range(0, len(x_d))))
    y_target_l = {}
    t_diff = {}
    target_per_week_dict = {}
    for key in list_of_projects:
        if date_FTU0[key] or date_FTU0[key] != '':
            t_start = x_prog[x_d == date_FTU0[key]][0]
            if date_FTU1[key]:
                t_max = x_prog[x_d == date_FTU1[key]][0]
            else:
                t_max = t_start + 100  # TODO: this is the ideal norm of 1%, we need to get this from the config
            t_diff[key] = t_max - t_start - 14  # two weeks round up
            slope_of_line = 100 / t_diff[key]  # target naar KPN is 100% HPend
        else:  # incomplete information on FTU dates
            t_start = 0
            slope_of_line = 0  # target naar KPN is 100% HPend
            t_diff[key] = 0

        b = -(slope_of_line * (t_start + 14))  # two weeks startup
        y_target = b + slope_of_line * x_prog
        y_target[y_target > 100] = 100
        y_target_l[key] = y_target

        # slope_of_line is the percentage of homes to be completed in a day (basically the target per day)
        target_per_week_dict[key] = slope_of_line / 100 * total_objects[key] * 7

    for key in y_target_l:
        y_target_l[key][y_target_l[key] > 100] = 100
        y_target_l[key][y_target_l[key] < 0] = 0

    return y_target_l, t_diff, target_per_week_dict


def get_map_bnumber_vs_project_from_sql():
    """
    This method extracts the bnumber, project name mapping table from the sql database.

    Returns:
            pd.DataFrame: a dataframe with bnumbers as keys and project names as values

    """
    sql_engine = get_database_engine()
    df = pd.read_sql('fc_baan_project_nr_name_map', sql_engine)
    ds_mapping = df[['fiberconnect_code', 'project_naam']].dropna().set_index('fiberconnect_code')
    ds_mapping.index = ds_mapping.index.astype(int).astype(str)
    ds_mapping = ds_mapping[~ds_mapping.duplicated()].rename(columns={'project_naam': 'project'})
    ds_mapping.index.name = 'bnummer'
    return ds_mapping


# TODO: check if this can be removed. It does not seem to be used.
def get_cumsum_of_col(df: pd.DataFrame, column):
    # Can we make it generic by passing column, or does df need to be filtered beforehand?
    filtered_df = df[~df[column].isna()]

    # Maybe we can move the rename of this column to preprocessing?
    agg_df = filtered_df.groupby([column]).agg({'sleutel': 'count'}).rename(columns={'sleutel': 'Aantal'})
    cumulative_df = agg_df.cumsum()

    return cumulative_df


# TODO: Documentation by Andre van Turnhout
def prognose(df: pd.DataFrame, t_s, x_d, tot_l, date_FTU0):  # noqa: C901
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


# # TODO: Documentation by Casper van Houten
# def fill_2020(df):
#     """
#     Function to
#     Args:
#         df:
#
#     Returns:
#
#     """
#     filler2020 = pd.DataFrame(index=pd.date_range(start='2020-01-01', end=df.index[0], freq='D'),
#                               columns=['d'],
#                               data=0)
#     df = pd.concat([filler2020[0:-1], df], axis=0)
#     return df


# # TODO: Documentation by Casper van Houten
# def percentage_to_amount(percentage, total):
#     return percentage / 100 * total


# # TODO: Documentation by Casper van Houten
# def transform_to_amounts(percentage_dict, total_dict, days_index):
#     df = pd.DataFrame(index=days_index, columns=['d'], data=0)
#     for key in percentage_dict:
#         amounts = percentage_to_amount(percentage_dict[key], total_dict[key])
#         df += pd.DataFrame(index=days_index, columns=['d'], data=amounts).diff().fillna(0)
#     if df.index[0] > pd.Timestamp('2020-01-01'):
#         df = fill_2020(df)
#     df = df['2020-01-01':]  # remove values from 2019, not required
#     return df
#
# # TODO: Documentation by Casper van Houten
# def transform_df_plan(x_d, HP):
#     df = pd.DataFrame(index=x_d, columns=['d'], data=0)
#     y_plan = pd.DataFrame(index=pd.date_range(start='30-12-2019', periods=len(HP['HPendT']), freq='W-MON'),
#                           columns=['d'], data=HP['HPendT'])
#     y_plan = y_plan.cumsum().resample('D').mean().interpolate().diff().fillna(y_plan.iloc[0])
#     df = df.add(y_plan, fill_value=0)
#     if df.index[0] > pd.Timestamp('2020-01-01'):
#         df = fill_2020(df)
#     df = df['2020-01-01':]  # remove values from 2019, not required
#     return df


# # TODO: Documentation by Casper van Houten
# def overview(x_d, y_prog_l, tot_l, d_real_l, HP, y_target_l):
#     df_prog = transform_to_amounts(y_prog_l, tot_l, x_d)
#     df_target = transform_to_amounts(y_target_l, tot_l, x_d)
#     df_real = transform_df_real(d_real_l, tot_l, x_d)
#     df_plan = transform_df_plan(x_d, HP)
#     OverviewResults = namedtuple("OverviewResults", ['df_prog', 'df_target', 'df_real', 'df_plan'])
#     return OverviewResults(df_prog, df_target, df_real, df_plan)


# def graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad, res, show_planning=True):
#     if 'W' in res:
#         n_now = int((pd.Timestamp.now() - pd.to_datetime('2019-12-30')).days / 7) + 1
#         n_d = int((pd.Timestamp.now() - pd.to_datetime('2020-' + str(datetime.date.today().month) + '-01')).days / 7)
#         x_ticks = list(range(n_now - n_d, n_now + 5 - n_d))
#         x_ticks_text = [datetime.datetime.strptime('2020-W' + str(int(el - 1)) + '-1', "%Y-W%W-%w").date().strftime(
#             '%Y-%m-%d') + '<br>W' + str(el) for el in x_ticks]
#         x_range = [n_now - n_d - 0.5, n_now + 4.5 - n_d]
#         y_range = [0, 3000]
#         width = 0.08
#         text_title = 'Maandoverzicht'
#         period = ['2019-12-23', '2020-12-27']
#         close = 'left'
#         loff = '-1W-MON'
#         x = df_prog[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum().index.week.to_list()
#         x[0] = 0
#     if 'M' == res:
#         n_now = datetime.date.today().month
#         x_ticks = list(range(0, 13))
#         x_ticks_text = ['dec', 'jan', 'feb', 'maa', 'apr', 'mei', 'jun', 'jul', 'aug', 'sep', 'okt', 'nov', 'dec']
#         x_range = [0.5, 12.5]
#         y_range = [0, 18000]
#         width = 0.2
#         text_title = 'Jaaroverzicht'
#         period = ['2019-12-23', '2020-12-27']
#         close = 'left'
#         loff = None
#         x = df_prog[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum().index.month.to_list()
#
#     prog0 = df_prog[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d']
#     prog = prog0.to_list()
#     target0 = df_target[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d']
#     target = target0.to_list()
#     real0 = df_real[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d']
#     real = real0.to_list()
#     plan0 = df_plan[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d']
#     plan = plan0.to_list()
#
#     if 'M' == res:
#         jaaroverzicht = dict(id='jaaroverzicht', target=str(round(sum(target))), real=str(round(sum(real))),
#                              plan=str(round(sum(plan[n_now - 1:]) - real[n_now - 1])),
#                              prog=str(round(sum(prog[n_now - 1:]) - real[n_now - 1])),
#                              HC_HPend=str(HC_HPend), HAS_werkvoorraad=str(HAS_werkvoorraad), prog_c='pretty_container')
#         if jaaroverzicht['prog'] < jaaroverzicht['plan']:
#             jaaroverzicht['prog_c'] = 'pretty_container_red'
#
#     bar_now = dict(x=[n_now],
#                    y=[y_range[1]],
#                    name='Huidige week',
#                    type='bar',
#                    marker=dict(color=colors['black']),
#                    width=0.5 * width,
#                    )
#     bar_t = dict(x=[el - 0.5 * width for el in x],
#                  y=target,
#                  name='Planning',
#                  type='bar',
#                  marker=dict(color=colors['lightgray']),
#                  width=width,
#                  )
#     bar_pr = dict(x=x,
#                   y=prog,
#                   name='Voorspelling (VQD)',
#                   mode='markers',
#                   marker=dict(color=colors['yellow'], symbol='diamond', size=15),
#                   #   width=0.2,
#                   )
#     bar_r = dict(x=[el + 0.5 * width for el in x],
#                  y=real,
#                  name='Realisatie (FC)',
#                  type='bar',
#                  marker=dict(color=colors['green']),
#                  width=width,
#                  )
#     bar_pl = dict(x=x,
#                   y=plan,
#                   name='Planning HP (VWT)',
#                   type='lines',
#                   marker=dict(color=colors['red']),
#                   width=width,
#                   )
#     fig = {
#         'data': [bar_pr, bar_pl, bar_r, bar_t, bar_now],
#         'layout': {
#             'barmode': 'stack',
#             #   'clickmode': 'event+select',
#             'showlegend': True,
#             'legend': {'orientation': 'h', 'x': -0.075, 'xanchor': 'left', 'y': -0.25, 'font': {'size': 10}},
#             'height': 300,
#             'margin': {'l': 5, 'r': 15, 'b': 10, 't': 40},
#             'title': {'text': text_title},
#             'xaxis': {'range': x_range,
#                       'tickvals': x_ticks,
#                       'ticktext': x_ticks_text,
#                       'title': ' '},
#             'yaxis': {'range': y_range, 'title': 'Aantal HPend'},
#             'plot_bgcolor': colors['plot_bgcolor'],
#             'paper_bgcolor': colors['paper_bgcolor'],
#             #   'annotations': [dict(x=x_ann, y=y_ann, text=jaaroverzicht, xref="x", yref="y",
#             #                   ax=0, ay=0, alignment='left', font=dict(color="black", size=15))]
#         },
#     }
#
#     prog0.index = prog0.index.strftime('%Y-%m-%d')
#     data_pr = dict(count_voorspellingdatum=prog0.to_dict())
#     target0.index = target0.index.strftime('%Y-%m-%d')
#     data_t = dict(count_outlookdatum=target0.to_dict())
#     real0.index = real0.index.strftime('%Y-%m-%d')
#     data_r = dict(count_opleverdatum=real0.to_dict())
#     plan0.index = plan0.index.strftime('%Y-%m-%d')
#     data_p = dict(count_hasdatum=plan0.to_dict())
#
#     if not show_planning:
#         data_p = dict.fromkeys(data_p, 0)
#
#     if 'W' in res:
#         record = dict(id='graph_targets_W', figure=fig)
#         return record, data_pr, data_t, data_r, data_p
#     if 'M' == res:
#         record = dict(id='graph_targets_M', figure=fig)
#         return record, data_pr, data_t, data_r, data_p


# # TODO: Documentation by Casper van Houten
# def slice_for_jaaroverzicht(data):
#     res = 'M'
#     close = 'left'
#     loff = None
#     period = ['2019-12-23', '2020-12-27']
#     return data[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d'].to_list()


# def preprocess_for_jaaroverzicht(*args):
#     return [slice_for_jaaroverzicht(arg) for arg in args]
#     # prog = slice_for_jaaroverzicht(df_prog)
#     # target = slice_for_jaaroverzicht(df_target)
#     # real = slice_for_jaaroverzicht(df_real)
#     # plan = slice_for_jaaroverzicht(df_plan)
#     # return prog, target, real, plan


# def calculate_jaaroverzicht(prognose, target, realisatie, planning, HAS_werkvoorraad, HC_HPend, bis_gereed):
#     n_now = datetime.date.today().month
#
#     target_sum = str(round(sum(target)))
#     planning_sum = sum(planning[n_now - 1:]) - realisatie[n_now - 1]
#     prognose_sum = sum(prognose[n_now - 1:]) - realisatie[n_now - 1]
#     realisatie_sum = str(round(sum(realisatie)))
#
#     jaaroverzicht = dict(id='jaaroverzicht',
#                          target=str(int(target_sum)),
#                          real=str(int(realisatie_sum)),
#                          plan=str(int(planning_sum)),
#                          prog=str(int(prognose_sum)),
#                          HC_HPend=str(HC_HPend),
#                          HAS_werkvoorraad=str(int(HAS_werkvoorraad)),
#                          bis_gereed=str(bis_gereed),
#                          prog_c='pretty_container')
#     if jaaroverzicht['prog'] < jaaroverzicht['plan']:
#         jaaroverzicht['prog_c'] = 'pretty_container_red'
#     return jaaroverzicht

# TODO: Documentation by Casper van Houten
def prognose_graph(x_d, y_prog_l, d_real_l, y_target_l, FTU0_date, FTU1_date):
    """
    Function that loops over all prognoses series, and turns them into a graphical representation per project.
    Should be refactored.
    Args:
        x_d: List of days
        y_prog_l: Dictionary of prognoses per project
        d_real_l: Dictionary of realised objectes per project
        y_target_l: Dictionary of target per project
        FTU0_date: Dictionary of FTU0 dates per project
        FTU1_date: Dictionary of FTU1 dates per project

    Returns: A dictionary of prognoses records per project, to be written to the firestore.

    """
    record_dict = {}
    for key in y_prog_l:
        if not FTU0_date[key]:  # TODO: Fix this -> replace with self.project_list
            FTU0_date[key] = '2020-01-01'
        if not FTU1_date[key]:  # some project do not have a FTU1 date assigned (yet), set project length to 100 days:
            FTU1_date[key] = (pd.to_datetime(FTU0_date[key]) + pd.Timedelta(days=100)).strftime('%Y-%m-%d')
        else:  # most projects have not finished at FTU1 date, adding 100 days to axis:
            FTU1_date[key] = (pd.to_datetime(FTU1_date[key]) + pd.Timedelta(days=100)).strftime('%Y-%m-%d')

        fig = {'data': [{
            'x': list(x_d.strftime('%Y-%m-%d')),
            'y': list(y_prog_l[key]),
            'mode': 'lines',
            'line': dict(color=colors['yellow']),
            'name': 'Voorspelling',
        }],
            'layout': {
                'xaxis': {'title': 'Opleverdatum [d]', 'range': [FTU0_date[key], FTU1_date[key]]},
                'yaxis': {'title': 'Opgeleverd HPend [%]', 'range': [0, 110]},
                'title': {'text': 'Voortgang project vs internal target:'},
                'showlegend': True,
                'legend': {'x': 0, 'xanchor': 'left', 'y': 1.15},
                'height': 450,
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
                'name': 'Realisatie HAS',
            }]

        if key in y_target_l:
            fig['data'] = fig['data'] + [{
                'x': list(x_d.strftime('%Y-%m-%d')),
                'y': list(y_target_l[key]),
                'mode': 'lines',
                'line': dict(color=colors['lightgray']),
                'name': 'Internal Target',
            }]
        record = dict(id='project_' + key, figure=fig)
        record_dict[key] = record
    return record_dict


# TODO: Documentation by Andre van Turnhout
def performance_matrix(x_d, y_target_l, d_real_l, tot_l, t_diff, current_werkvoorraad):
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
            y += [round(current_werkvoorraad[key] / y_voorraad * 100)]
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
                   'xaxis': {'title': 'Procent voor of achter HPEnd op Internal Target', 'range': [x_min, x_max],
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
                                        text='Verruim klantafspraak',
                                        alignment='left', showarrow=True, arrowhead=2)] +
                                  [dict(x=13.5, y=160, ax=100, ay=0, xref="x", yref="y",
                                        text='Verlaag HAS capcaciteit',
                                        alignment='right', showarrow=True, arrowhead=2)] +
                                  [dict(x=13.5, y=40, ax=100, ay=0, xref="x", yref="y",
                                        text='Verscherp klantafspraak',
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


def create_project_filter(df: pd.DataFrame):
    """
    Creates a filter based on the projects in the dataframe.
    Args:
        df (pd.DataFrame): A dataframe containing a categorical projects column.

    Returns:
        list[dict]: A list of dictionaries with the projects with the following shape
        {'label': project, 'value': project}
    """
    filters = [{'label': x, 'value': x} for x in df.project.cat.categories]
    record = dict(filters=filters)
    return record


# TODO: Documentation by Andre van Turnhout
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


def calculate_week_target(project: str, target_per_week: dict, FTU0: dict, FTU1: dict, time_delta_days: int = 0) -> int:
    """
    Calculates the target per week for a project. Based on the richtings_coefficient and total_objects calculated
    in functions.py: targets().

    Args:
        project: project
        target_per_week: target per week per project
        FTU0: startdate per project
        FTU1: enddate per project
        time_delta_days: optional argument to calculate the target for last week

    Returns:
        The target per week for a project

    """
    # TODO this target information (pd.Timedelta(days=100) must be stored in the config
    if FTU0[project]:
        if not FTU1[project]:
            FTU1[project] = pd.Timestamp(FTU0[project]) + pd.Timedelta(days=100)
            FTU1[project] = FTU1[project].strftime("%Y-%m-%d")

        if (pd.to_datetime(FTU0[project]) < (pd.Timestamp.now() - pd.Timedelta(days=time_delta_days))) \
                & \
                ((pd.Timestamp.now() - pd.Timedelta(days=time_delta_days)) < pd.to_datetime(FTU1[project])):
            target = int(round(target_per_week[project]))
        else:
            target = 0
    else:
        target = 0
    return target


def _create_bullet_chart_realisatie(value: float,
                                    prev_value: float,
                                    max_value: float,
                                    yellow_border: float,
                                    threshold: float,
                                    title: str = "",
                                    subtitle: str = ""):
    """
    Creates a bullet chart to be rendered by `Plotly <https://plotly.com/python/indicator/#bullet-gauge>`_.

    Args:
        value (float): Current value
        prev_value (float): Previous value, adds a delta to the bullet chart
        max_value (float): Maximum value in the range of the bullet chart
        yellow_border (float): Maximum value for the yellow area
        threshold (float): Value for the red line
        title (str): Title, optional
        subtitle (str): Subtitle, optional

    Returns:
        dict: A plotly graph object
    """
    return dict(counts=value,
                counts_prev=prev_value,
                title=title,
                subtitle=subtitle,
                font_color='green',
                gauge={
                    'shape': "bullet",
                    'axis': {'range': [0, max_value]},
                    'threshold': {
                        'line': {'color': "red", 'width': 2},
                        'thickness': 0.75,
                        'value': threshold},
                    'steps': [
                        {'range': [0, yellow_border], 'color': "yellow"},
                        {'range': [yellow_border, max_value], 'color': "lightgreen"}]},
                id=None)


def lastweek_realisatie_hpend_bullet_chart(project_df, weektarget):
    """
    Calculate the HPend realisatie and target for last week and visualize in a bullet chart.

    Args:
        project_df (pd.DataFrame): Project dataframe containing an opleverdatum.
        weektarget (float): The target for HPend per week

    Returns:
        dict: A plotly bullet chart
    """
    weekday = datetime.datetime.now().weekday()
    realisatie_end_week = br.opgeleverd(project_df, weekday).sum()
    realisatie_beginning_week = br.opgeleverd(project_df, weekday + 1 + 7).sum()

    realisatie = int(realisatie_end_week - realisatie_beginning_week)

    max_value = int(max(weektarget, realisatie, 1) * 1.1)
    return _create_bullet_chart_realisatie(value=realisatie,
                                           prev_value=None,
                                           max_value=max_value,
                                           yellow_border=int(weektarget * 0.9),
                                           threshold=max(weektarget, 0.01),  # 0.01 to show a 0 threshold
                                           title=f'HAS realisatie week {int(datetime.datetime.now().strftime("%V")) - 1}',
                                           subtitle=f"Target: {weektarget}")


def thisweek_realisatie_hpend_bullet_chart(project_df, weektarget, delta=0):
    """
    Calculate the HPend realisatie and target for last week and visualize in a bullet chart.

    Args:
        project_df (pd.DataFrame): Project dataframe containing an opleverdatum.
        weektarget (float): The target for HPend per week
        delta: The number of days to shift the realisatie.

    Returns:
        dict: A plotly bullet chart

    """
    weekday = datetime.datetime.now().weekday()
    realisatie_beginning_week = br.opgeleverd(project_df, weekday + 1 + delta).sum()

    realisatie_today = br.opgeleverd(project_df, 0 + delta).sum()
    realisatie_this_week = int(realisatie_today - realisatie_beginning_week)

    realisatie_yesterday = br.opgeleverd(project_df, 1 + delta).sum()
    realisatie_this_week_yesterday = int(realisatie_yesterday - realisatie_beginning_week)

    max_value = int(max(weektarget, realisatie_this_week, 1) * 1.1)
    return _create_bullet_chart_realisatie(value=realisatie_this_week,
                                           prev_value=realisatie_this_week_yesterday,
                                           max_value=max_value,
                                           yellow_border=int(weektarget * 0.9),
                                           threshold=max(weektarget, 0.01),  # 0.01 to show a 0 threshold
                                           title=f'HAS realisatie week {int(datetime.datetime.now().strftime("%V"))}',
                                           subtitle=f"Target:{weektarget}")


def week_realisatie_bullet_chart(week_realisatie, week_realisatie_day_before, week_target, week_delta=0):
    """
        Creates bullet chart using targets and realisaties
    Args:
        week_realisatie: realisatie
        week_realisatie_day_before: int, realisatie previous day
        week_target: int, target for the week
        week_delta: shift weeknumber to display backwards (not sustainable in weeks 1)
    Returns:
        dict: A plotly bullet chart

    """
    max_value = int(max(week_target, week_realisatie, 1) * 1.1)
    return _create_bullet_chart_realisatie(value=week_realisatie,
                                           prev_value=week_realisatie_day_before,
                                           max_value=max_value,
                                           yellow_border=int(week_target * 0.9),
                                           threshold=max(week_target, 0.01),  # 0.01 to show a 0 threshold
                                           title=f'BIS realisatie week {int(datetime.datetime.now().strftime("%V"))-week_delta}',
                                           subtitle=f"Bis target: {week_target}")


def make_graphics_for_ratio_hc_hpend_per_project(project: str, ratio_HC_HPend_per_project: dict):
    """
    This function takes in a dictionary with HC/HPend ratios per project and a project of interest. It then returns
    a dictionary that has all the graphical layout components necessary to plot the HC/HPend value on the dashboard.

    Args:
        project: the project of interest (str)
        ratio_HC_HPend_per_project: a dictionary with HC/HPend values per project (dict)

    Returns:
        a dictionary that contains the graphical layout to plot this value on the dashboard

    """
    counts = round(ratio_HC_HPend_per_project[project], 2)

    return dict(title='HC / HPend',
                subtitle='',
                counts=counts,
                counts_prev=None,
                font_color='green',
                gauge={
                    'axis': {'range': [None, 1], 'tickwidth': 1, 'tickcolor': "green"},
                    'bar': {'color': "darkgreen"},
                    'bgcolor': "white",
                    'borderwidth': 2,
                    'bordercolor': "gray",
                    'steps': [
                        {'range': [0, .6], 'color': 'yellow'},
                        {'range': [.6, 1], 'color': 'lightgreen'}],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': .9}
                },
                id=None)


def make_graphics_for_number_errors_fcbc_per_project(project: str, number_errors_per_project: dict):
    """
    This function takes in a dictionary with the number of errors between FiberConnect (FC) and KPN's Beschikbaarheid
    Checker (BC) per project and a project of interest. It then returns a dictionary that has all the graphical layout
    components necessary to plot the errors between FC/BC on the dashboard.

    Args:
        project: the project of interest (str)
        ratio_HC_HPend_per_project: a dictionary with error between FC/BC values per project (dict)

    Returns:
        a dictionary that contains the graphical layout to plot this value on the dashboard

    """
    return dict(counts=number_errors_per_project[project],
                counts_prev=None,
                title='Errors FC- BC',
                subtitle='',
                font_color='green',
                id=None)


def calculate_current_werkvoorraad(df: pd.DataFrame):
    """
    Calculates the current werkvoorraad per project.
    Args:
        df (pd.DataFrame):  A dataframe containing the following columns:
        [schouwdatum, opleverdatum, toestemming_datum, opleverstatus]

    Returns:
        dict: A dictionary with the project as key and the werkvoorraad as the value.
    """
    # todo add in_has_werkvoorraad column in etl and use that column
    return df[br.has_werkvoorraad(df)] \
        .groupby(by="project") \
        .count() \
        .reset_index()[['project', "sleutel"]] \
        .set_index("project").to_dict()['sleutel']


# TODO: Documentation by Andre van Turnhout. Perhaps remove?
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


# TODO: Documentation by Andre van Turnhout. Perhaps remove?
def add_token_mapbox(token):
    # TODO, remove if in secrets
    record = dict(id='token_mapbox',
                  token=token)
    firestore.Client().collection('Graphs').document(record['id']).set(record)


def set_date_update(client=None):
    """
    This functions sets the date for the last time the analysis function has run correctly
    Since we have disinct analysis functions for each client, the update date is set for a
    specific client.

    Args:
        client: client name

    Returns: timestamp store in a document for the last correct run of the analysis

    """
    id_ = f'update_date_{client}' if client else 'update_date'
    record = dict(id=id_, date=pd.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))
    firestore.Client().collection('Graphs').document(record['id']).set(record)


# TODO: Documentation by Andre van Turnhout
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

    def check_numeric_and_lenght(series: pd.Series, min_length=1, max_length=100, fillna=True):
        """
        Checks if the number of digits is within a range. Empty values will be evaluated the fillna parameter describes.
        Args:
            series: A series of values
            min_length: Minimal length
            max_length: Maximum length
            fillna: True or False.

        Returns:

        """
        return (series.str.len() > max_length) | (series.str.len() < min_length) | ~(
            series.str.isnumeric().fillna(fillna))

    business_rules['509'] = check_numeric_and_lenght(df.kastrij, max_length=2)
    business_rules['510'] = check_numeric_and_lenght(df.kast, max_length=4)
    business_rules['511'] = check_numeric_and_lenght(df.odf, max_length=5)
    business_rules['512'] = check_numeric_and_lenght(df.odfpos, max_length=2)
    business_rules['513'] = check_numeric_and_lenght(df.catv, max_length=5)
    business_rules['514'] = check_numeric_and_lenght(df.catvpos, max_length=3)

    business_rules['516'] = no_errors_series  # cannot check
    business_rules['517'] = no_errors_series  # date is already present in different format...yyyy-mm-dd??
    business_rules['518'] = (~df.toestemming.isin(['Ja', 'Nee', np.nan, None]))
    business_rules['519'] = (~df.soort_bouw.isin(['Laag', 'Hoog', 'Duplex', 'Woonboot', 'Onbekend']))
    business_rules['520'] = ((df.ftu_type.isna() & df.opleverstatus.isin(['2', '10'])) | (~df.ftu_type.isin(
        ['FTU_GN01', 'FTU_GN02', 'FTU_PF01', 'FTU_PF02', 'FTU_TY01', 'FTU_ZS_GN01', 'FTU_TK01', 'Onbekend'])))
    business_rules['521'] = (df.toelichting_status.str.len() < 3)
    business_rules['522'] = no_errors_series  # Civieldatum not present in our FC dump
    business_rules['524'] = no_errors_series  # Kavel not present in our FC dump
    business_rules['527'] = no_errors_series  # HL opleverdatum not present in our FC dump
    business_rules['528'] = (~df.redenna.isin(
        [np.nan, None, 'R0', 'R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'R9', 'R10', 'R11', 'R12', 'R13', 'R14',
         'R15',
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
    """
    Retrieves the relevant cluster of a label, given a set of clusters.
    Args:
        label: Current, unclustered label of the data
        clusters: A dictionary of clusters, keys being the name of the cluster, values being the labels in the cluster.

    Returns: The cluster of the data.

    """
    for k, v in clusters.items():
        if label in v:
            return k
    # raise ValueError(f'No label found for {label}')


def pie_chart_reden_na(df_na, key):
    """
    Creates a pie chart for reden_na
    Args:
        df_na: Dataframe with reden_na
        key: Name of the project

    Returns: Record to be written to the firestore, along with the name of the document.

    """
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


def overview_reden_na(df: pd.DataFrame):
    """
    Wrapper function to create the record for overview reden na.
    Args:
        df: Dataframe that contains data for all projects in FC
        clusters: Clusters that the reden_na will be discriminated to, and will be used in the pie chart

    Returns: Record to be written to the firestore.

    """
    data, document = pie_chart_reden_na(df, 'overview')
    layout = get_pie_layout()
    fig = dict(data=data, layout=layout)
    record = dict(id=document, figure=fig)
    return record


def individual_reden_na(df: pd.DataFrame):
    """
    Wrapper function to create the records for reden na per project.
    Args:
        df: Dataframe that contains data for all projects in FC
        clusters: Clusters that the reden_na will be discriminated to, and will be used in the pie chart

    Returns: Record to be written to the firestore.

    """
    record_dict = {}
    for project, df in df.groupby(by="project"):
        data, document = pie_chart_reden_na(df, project)
        record = dict(id=document, data=data)
        record_dict[document] = record
    return record_dict


def get_pie_layout():
    """
    Getter for the layout of the reden_na pie chart
    Returns: Layout for reden_na pie chart.

    """
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


def calculate_redenna_per_period(df: pd.DataFrame, date_column: str = 'hasdatum', freq: str = 'W-MON') -> dict:
    """
    Calculates the number of each reden na cluster (as defined in the config) grouped by
    the date of the 'date_column'. The date is grouped in buckets of the period. For example by week or month.

    Set the freq using:
    `Offset aliases <https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases>`_

    We commonly use: \n
    -   'W-MON' for weeks starting on Monday. (label = monday)
    -   'M' for month (label is the last day of the period)
    -   'Y' for year (label is the last day of the period)

    Args:
        df: The data set
        date_column: The column used to group on
        freq: The period to use in the grouper

    Returns:
        dict: a dictionary with the first day of the period as key,
        and the clusters with their occurrence counts as value.

    """
    if freq == 'W-MON':
        label_side = 'left'
        closed_side = 'left'
    if freq == 'M' or freq == 'Y':
        label_side = 'right'
        closed_side = 'right'

    redenna_period_df = df[['cluster_redenna', date_column, 'project']] \
        .groupby(by=[pd.Grouper(key=date_column,
                                freq=freq,
                                closed=closed_side,  # closed end of the interval, see:
                                # (https://en.wikipedia.org/wiki/Interval_(mathematics)#Terminology)
                                label=label_side  # label specifies whether the result is labeled
                                # with the beginning or the end of the interval.
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

    Args:
        rules_list: A list of masks for a particular datafame.
        state_list: The states that the rules describe

    Returns:
        pd.Series: A series of the state for each row in the dataframe.
    """
    if len(rules_list) != len(state_list):
        raise ValueError("The number of rules must be equal to the number of states")
    calculation_df = pd.concat(rules_list, axis=1).astype(int)
    state = calculation_df.apply(
        lambda x: state_list[list(x).index(True)],
        axis=1
    )
    return state


# TODO: Documentation by  Andre van Turnhout, add the arguments and return value   -> can this be removed?
def wait_bins(df: pd.DataFrame, time_delta_days: int = 0) -> pd.DataFrame:
    """
    This function counts the wait between toestemming datum and now (or a reference date, based on time_delta_days).
    It only considers houses which are not connected yet.

    Args:
        df:
        time_delta_days:

    Returns:

    """
    time_point = (pd.Timestamp.today() - pd.Timedelta(days=time_delta_days))
    mask = ~br.opgeleverd(df, time_delta_days) & br.toestemming_bekend(df)
    toestemming_df = df[mask][['toestemming', 'toestemming_datum', 'opleverdatum', 'cluster_redenna']]

    toestemming_df['waiting_time'] = (time_point - toestemming_df.toestemming_datum).dt.days / 7
    toestemming_df['bins'] = pd.cut(toestemming_df.waiting_time,
                                    bins=[-np.inf, 0, 8, 12, np.inf],
                                    labels=['before_order', 'on_time', 'limited_time', 'late'])
    return toestemming_df


# TODO: Documentation by Andre van Turnhout  -> can this be removed?
def count_toestemming(toestemming_df):
    toestemming_df = toestemming_df.rename(columns={'bins': "counts"})
    counts = toestemming_df.counts.value_counts()
    return counts


# TODO: Documentation by Andre van Turnhout  -> can this be removed?
def wait_bin_cluster_redenna(df):
    wait_bin_cluster_redenna_df = df[['wait_category', 'cluster_redenna', 'toestemming']].groupby(
        by=['wait_category', 'cluster_redenna']).count()
    wait_bin_cluster_redenna_df = wait_bin_cluster_redenna_df.rename(columns={"toestemming": "count"})
    wait_bin_cluster_redenna_df = wait_bin_cluster_redenna_df.fillna(value={'count': 0})
    return wait_bin_cluster_redenna_df


def calculate_projectindicators_tmobile(df: pd.DataFrame, has_werkvoorraad_per_project: dict,
                                        time_windows_per_project: dict, ratio_under_8weeks_per_project: dict):
    """
    This function takes in the transformed_data DataFrame and three dictionairies, containing the HAS werkvoorrraad,
    openstaande orders per timewindow and ratio <8 weeks aangesloten orders values per project, respectively. The
    function first generates a markup_dict, which contains all the graphical layout components necessary to plot
    these values (specific for tmobile) on the dashboard.

    Args:
        df: the transformed_data DataFrame (pd.DataFrame)
        has_werkvoorraad_per_project: a dictionary with HAS werkvoorraad values per project (dict)
        time_windows_per_project: a dictionary with openstaande orders per timewindow per project (dict)
        ratio_under_8weeks_per_project: a dictionary with ratio <8 weeks aangesloten orders per project (dict)

    Returns:
         a dictionary that contains the graphical layout to plot tmobile values on the dashboard

    """
    markup_dict = {
        'on_time-patch_only': {'title': 'Openstaand patch only op tijd',
                               'subtitle': '< 8 weken',
                               'font_color': 'green'},
        'limited-patch_only': {'title': 'Openstaand patch only beperkte tijd',
                               'subtitle': '> 8 weken < 12 weken',
                               'font_color': 'orange'},
        'late-patch_only': {'title': 'Openstaand patch only te laat',
                            'subtitle': '> 12 weken',
                            'font_color': 'red'},

        'ratio': {'title': 'Ratio op tijd gesloten orders',
                  'subtitle': '<12 weken',
                  'font_color': 'black',
                  'percentage': True},
        'before_order': {'title': '', 'subtitle': '', 'font_color': ''},
        'ready_for_has': {'title': "Werkvoorraad HAS"},

        'on_time-hc_aanleg': {'title': 'Openstaand HC aanleg op tijd',
                              'subtitle': '< 8 weken',
                              'font_color': 'green'},
        'limited-hc_aanleg': {'title': 'Openstaand HC aanleg beperkte tijd',
                              'subtitle': '> 8 weken < 12 weken',
                              'font_color': 'orange'},
        'late-hc_aanleg': {'title': 'Openstaand HC aanleg te laat',
                           'subtitle': '> 12 weken',
                           'font_color': 'red'}
    }

    counts_by_project = {}
    for project, project_df in df.groupby(by='project'):
        counts_by_project[project] = {}

        counts_by_project[project].update({'ready_for_has': has_werkvoorraad_per_project[project]})
        counts_by_project[project].update({'late-patch_only': time_windows_per_project[project]['openstaand_patch_only_late']})
        counts_by_project[project].update({'on_time-patch_only': time_windows_per_project[project]['openstaand_patch_only_on_time']})
        counts_by_project[project].update({'limited-patch_only': time_windows_per_project[project]['openstaand_patch_only_limited']})
        counts_by_project[project].update({'before_order': {'counts': 0,
                                                            'counts_prev': 0,
                                                            'cluster_redenna': {'HC': 0,
                                                                                'geplande aansluiting': 0,
                                                                                'permissieobstructies': 0,
                                                                                'technische obstructies': 0}}})
        counts_by_project[project].update({'ratio': {'counts': ratio_under_8weeks_per_project[project]}})
        counts_by_project[project].update({'late-hc_aanleg': time_windows_per_project[project]['openstaand_hc_aanleg_late']})
        counts_by_project[project].update({'on_time-hc_aanleg': time_windows_per_project[project]['openstaand_hc_aanleg_on_time']})
        counts_by_project[project].update({'limited-hc_aanleg': time_windows_per_project[project]['openstaand_hc_aanleg_limited']})

        for indicator, markup in markup_dict.items():
            counts_by_project[project][indicator].update(markup)
    return counts_by_project


# # TODO: Documentation by Casper van Houten
# def calculate_on_time_ratio(df):
#     # Maximum days an order is allowed to take in days
#     max_order_time = 56
#     ordered = df[df.ordered & df.opgeleverd]
#     on_time = ordered[ordered.oplevertijd <= max_order_time]
#     try:
#         on_time_ratio = len(on_time) / len(ordered)
#     except ZeroDivisionError:
#         on_time_ratio = 0
#     return on_time_ratio


# TODO: Documentation by Casper van Houten
def calculate_oplevertijd(row):
    """
    Calculates the oplevertijd, which is the amount of days between the toestemmingsdatum and opleverdatum.
    Used for T-mobile, as it uses 'ordered' logic, which only applies to T-mobile.
    Args:
        row: row of data, including exactly one opleverdatum and toestemmingsdatum

    Returns: the oplevertijd in days, as an integer.
    will return NaN if woning does not have state 'opgeleverd' yet, or if the row has not been ordered yet.

    """
    # Do not calculate an oplevertijd if row was not ordered or not opgeleverd
    if row.ordered and row.opgeleverd:
        oplevertijd = (row.opleverdatum - row.toestemming_datum).days
    else:
        oplevertijd = np.nan
    return oplevertijd

# def calculate_bis_gereed(df):
#     df_copy = df.copy()
#     df_copy = df_copy.loc[(df_copy.opleverdatum >= pd.Timestamp('2020-01-01')) | (df_copy.opleverdatum.isna())]
#     return sum(br.bis_opgeleverd(df_copy))


# TODO: Documentation by Casper van Houten
def linear_regression(data):
    fit_range = data.day_count.to_list()
    slope, intersect = np.polyfit(fit_range, data, 1)
    return slope[0], intersect[0]


# TODO: Documentation by Casper van Houten
def multi_index_to_dict(df):
    project_dict = {}
    for project in df.columns.get_level_values(0).unique():
        idx = pd.IndexSlice
        data = df.loc[idx[:], idx[project, :]][project]
        project_dict[project] = data
    return project_dict


def extract_werkvoorraad_has_dates(df: pd.DataFrame, time_delta_days: int = 0, add_project_column: bool = False):
    """
    This function extracts the werkvoorraad HAS dates per client from their transformed dataframes, based on the BR:
    has_werkvoorraad (see BR) and the latest date between: schouwdatum, toestemming_datum and status_civiel_datum.

    Args:
        df: The transformed dataframe
        time_delta_days (int): optional, the number of days before today.
        add_project_column (bool): Optional argument to return project column in addition to datecolumn

    Returns:
        pd.Series

    """
    if add_project_column:
        time_dataseries = df[br.has_werkvoorraad(df, time_delta_days)][
            ['schouwdatum', 'toestemming_datum', 'status_civiel_datum']].max(axis=1)
        time_dataseries.name = 'werkvoorraad_has_datum'
        project_dataseries = df[br.has_werkvoorraad(df, time_delta_days)].project
        df = pd.concat([time_dataseries, project_dataseries], axis=1)
        return df
    else:
        ds = df[br.has_werkvoorraad(df, time_delta_days)][
            ['schouwdatum', 'toestemming_datum', 'status_civiel_datum']].max(axis=1)
        ds.name = 'werkvoorraad_has_datum'
        return ds


def extract_realisatie_hpend_dates(df: pd.DataFrame, add_project_column: bool = False):
    """
    This function extracts the realisatie HPend dates per client from their transformed dataframes, based on the BR:
    hpend_opgeleverd (opleverdatum has been set) and the date: opleverdatum.

    Args:
        df (pd.DataFrame): The transformed dataframe
        add_project_column (bool): Optional argument to return project column in addition to datecolumn

    Returns:
        pd.Series, pd.DataFrame: A pd.Series or pd.DataFrame dependent on state of add_project_column

    """
    if add_project_column:
        return df[br.hpend(df)][['opleverdatum', 'project']]
    else:
        return df[br.hpend(df)].opleverdatum


def extract_aangesloten_orders_dates(df: pd.DataFrame) -> pd.Series:
    """
    This function extracts the realisatie HPend dates that have been ordered per client from their transformed
    dataframes, based on the BR: hpend_opgeleverd_and_ordered (opleverdatum and ordered are present) and the date:
    opleverdatum. The 'ordered' column is only available for tmobile and is necessary to calculate the HPend houses
    that have been actually ordered (instead of the total HPend houses).

    Args:
        df (pd.DataFrame): The transformed dataframe

    Returns:
        pd.Series

    """
    if 'ordered' in df.columns:
        return df[br.aangesloten_orders_tmobile(df)].opleverdatum
    else:
        return df[br.hpend(df)].opleverdatum


def extract_realisatie_hc_dates(df: pd.DataFrame, add_project_column: bool = False):
    """
    This function extracts the realisatie HC dates per client from their transformed dataframes, based on the BR:
    hc_opgeleverd (opleverstatus == 2) and the date: opleverdatum.

    Args:
        df (pd.DataFrame): The transformed dataframe
        add_project_column (bool): Optional argument to return project column in addition to datecolumn

    Returns:
        pd.Series, pd.DataFrame: A pd.Series or pd.DataFrame dependent on state of add_project_column

    """
    if add_project_column:
        return df[br.hc_opgeleverd(df)][['opleverdatum', 'project']]
    else:
        return df[br.hc_opgeleverd(df)].opleverdatum


def extract_voorspelling_dates(df: pd.DataFrame, ftu=None, totals=None) -> pd.Series:
    """
    This function extracts the voorspelling dates per client from their transformed dataframes. For tmobile no
    voorspelling is done yet. For KPN, the voorspelling date is extracted via the extract_voorspelling_dates_kpn
    function, which needs a declared ftu column and totals column in addition to the DataFrame.

    Args:
        df: The transformed dataframe
        ftu: Andre van Turnhout
        totals: Andre van Turnhout

    Returns:
         A pd.Series object

    """
    if ftu and any(ftu.get('date_FTU0', {}).values()):
        return extract_voorspelling_dates_kpn(
            df=df,
            start_time=get_start_time(df),
            timeline=get_timeline(get_start_time(df)),
            totals=totals,
            ftu=ftu['date_FTU0']
        )
    else:
        df_prog = pd.DataFrame(index=get_timeline(get_start_time(df)), columns=['prognose'], data=0)
        return df_prog.prognose


# TODO: Documentation by Andre van Turnhout
def extract_voorspelling_dates_kpn(df: pd.DataFrame, start_time, timeline, totals, ftu):
    """
    Andre

    Args:
        df:
        start_time:
        timeline:
        totals:
        ftu:

    Returns:

    """
    result = prognose(df,
                      start_time,
                      timeline,
                      totals,
                      ftu)
    df_prog = pd.DataFrame(index=timeline, columns=['prognose'], data=0)
    for key in result.y_prog_l:
        amounts = result.y_prog_l[key] / 100 * totals[key]
        df_prog += pd.DataFrame(index=timeline, columns=['prognose'], data=amounts).diff().fillna(0)
    return df_prog.prognose


def extract_planning_dates(df: pd.DataFrame, client: str, planning: dict = None) -> pd.Series:
    """
    This function extracts the planning dates per client from their transformed dataframes. For KPN a planning column
    can be supplied, which is obtained from an excel sheet from Wout.

    Args:
        df: The transformed dataframe
        client: Either 'kpn', 'tmobile' or 'dfn'
        planning: Optional column for kpn, not used anymore

    Returns:
         A pd.Series object

    """
    use_kpn_planning_excel = True
    if planning and client == 'kpn' and use_kpn_planning_excel is True:
        return extract_planning_dates_kpn(data=planning['HPendT'], timeline=get_timeline(get_start_time(df)))
    else:
        return df[~df.hasdatum.isna()].hasdatum


def extract_planning_dates_kpn(data: list, timeline: pd.DatetimeIndex):
    """
    This function extracts the planning HPend value from the planning supplied by Wout. It loads in the data, which
    contains the planning values per week and converts it into a pd.DataFrame.

    Args:
        data: planning per week values from the kpn planning excel
        timeline: a datetimeindex running from x to y

    Returns: a pd.DataFrame containing the planning values per day.

    """
    df = pd.DataFrame(index=timeline, columns=['planning_kpn'], data=0)
    if data:
        planning_df = pd.DataFrame(index=pd.date_range(start='2021-01-04', periods=len(data), freq='W-MON'),
                                   columns=['planning_kpn'], data=data)
        planning_df = planning_df / 5  # divides the weekly values to daily values: no work is done in the weekend
        planning_df = planning_df.resample('D').ffill(limit=4)  # upsamples the weekly values into daily values
        df = df.add(planning_df, fill_value=0)
    return df.planning_kpn


def extract_target_dates(df: pd.DataFrame, project_list, ftu=None, totals=None):
    """
    This function extracts the target dates per client from their transformed dataframes. The target is calculated
    differently for KPN/DFN than for tmobile: when a ftu and totals column is declared in addition to the DataFrame,
    the function for KPN/DFN is used, otherwise the function for tmobile is used.

    Args:
        df: The transformed dataframe
        ftu: Andre
        totals: Andre

    Returns:
        pd.Series

    """
    if ftu and any(ftu.get('date_FTU0', {}).values()):
        return extract_target_dates_kpn(
            timeline=get_timeline(get_start_time(df)),
            totals=totals,
            project_list=project_list,
            ftu0=ftu['date_FTU0'],
            ftu1=ftu['date_FTU1'])
    else:
        return df[br.target_tmobile(df)].creation


# TODO: Documentation by Andre van Turnhout
def extract_target_dates_kpn(timeline, totals, project_list, ftu0, ftu1):
    """
    Andre

    Args:
        timeline:
        totals:
        project_list:
        ftu0:
        ftu1:

    Returns:

    """
    y_target_l, _, _ = targets_new(timeline, project_list, ftu0, ftu1, totals)
    df_target = pd.DataFrame(index=timeline, columns=['target'], data=0)
    for key in y_target_l:
        amounts = y_target_l[key] / 100 * totals[key]
        df_target += pd.DataFrame(index=timeline, columns=['target'], data=amounts).diff().fillna(0)
    return df_target.target


def get_secret(project_id, secret_id, version_id='latest'):
    client = secretmanager.SecretManagerServiceClient()
    name = client.secret_version_path(project_id, secret_id, version_id)
    response = client.access_secret_version(name)
    payload = response.payload.data.decode('UTF-8')
    return payload


def get_database_engine():
    """
    Construct an SQLAlchemy Engine based on the config file.

    Returns:
        An SQLAlchemy Engine instance
    """

    if 'db_ip' in config.database:
        SACN = 'mysql+mysqlconnector://{}:{}@{}:3306/{}?charset=utf8&ssl_ca={}&ssl_cert={}&ssl_key={}'.format(
            config.database['db_user'],
            get_secret(config.database['project_id'], config.database['secret_name']),
            config.database['db_ip'],
            config.database['db_name'],
            config.database['server_ca'],
            config.database['client_ca'],
            config.database['client_key']
        )
    else:
        SACN = 'mysql+pymysql://{}:{}@/{}?unix_socket=/cloudsql/{}:europe-west1:{}'.format(
            config.database['db_user'],
            get_secret(config.database['project_id'], config.database['secret_name']),
            config.database['db_name'],
            config.database['project_id'],
            config.database['instance_id']
        )

    return create_engine(SACN, pool_recycle=3600)


# TODO: Documentation by Andre van Turnhout
def sum_over_period(data: pd.Series, freq: str, period=None) -> pd.Series:
    """
    Set the freq using: https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
    We commonly use: \n
    -    'W-MON' for weeks starting on Monday. (label = monday)
    -    'M' for month (label is the last day of the period)
    -    'Y' for year (label is the last day of the period)

    Args:
        data: A pd.Series to sum over
        freq: The period to use in resample
        period:

    Returns:
        pd.Series

    """
    if data is None:
        data = pd.Series()

    if freq == 'W-MON':  # interval labels: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.resample.html
        label_side = 'left'
        closed_side = 'left'
    if freq == 'M' or freq == 'Y':
        label_side = 'right'
        closed_side = 'right'

    if not data[~data.isna()].empty:
        if not isinstance(data.index[0], pd.Timestamp):
            data = data.groupby(data).count()

    if period:
        data_filler = pd.Series(index=pd.date_range(start=period[0], end=period[1], freq=freq), name=data.name, data=0)
        if not data[~data.isna()].empty:
            data_counted = (data_filler + data.resample(freq, closed=closed_side, label=label_side).sum()[
                                          period[0]:period[1]]).fillna(0)
        else:
            data_counted = data_filler
    else:
        if not data[~data.isna()].empty:
            data_counted = data.resample(freq, closed=closed_side, label=label_side).sum()
        else:
            data_counted = pd.Series()

    return data_counted


def sum_over_period_to_record(timeseries: pd.Series, freq: str, year: str):
    """
    This function takes a timeseries, sums the series over a defined period (either annual, monthly or weekly),
    converts the result to a dictionary and returns a record ready for the firestore

    Args:
        timeseries: A pd.Series
        freq: Either 'W-MON', 'M' or 'Y'
        year: The year to sum over

    Returns:
        Record: Record for the firestore
    """
    data = sum_over_period(timeseries, freq, period=[year + '-01-01', year + '-12-31'])
    data.index = data.index.format()
    record = data.to_dict()
    return record


def convert_graafsnelheid_from_m_week_to_woning_day(total_meters_bis: float, total_num_has: float, snelheid_m_week: float):
    """
    Calculates target speed (woning/dag) by given graafsnelheid in meters per week, total meters bis and total
    number huisaansluitingen.

    Args:
        total_meters_bis: total meter graven bis in a project
        total_num_has: total number of huisaansluitingen in a project
        snelheid_m_week: graafsnelheid (meters per week) specified for a project
    Returns:
        speed_woning_per_day: target number of woningen to pass per day in a project
    """
    snelheid_m_day = snelheid_m_week / 7
    meter_per_woning = total_meters_bis / total_num_has
    speed_woning_per_day = snelheid_m_day / meter_per_woning
    return speed_woning_per_day


def calculate_graafsnelheid_woning_day_by_percentage_norm(total_num_has: float):
    """
    Calculates target speed in (woning/dag) by norm percentage of total woningen to be done per day and total
    number of huisaansluitingen.

    Args:
        total_num_has: total number of huisaansluitingen in a project
    Returns:
        speed_woning_per_day: target number of woningen to pass per day in a project
    """
    speed_woning_per_day = config.perc_norm_bis * total_num_has
    return speed_woning_per_day


def calculate_graafsnelheid_woning_day(total_meters_bis: float, total_num_has: float, snelheid_m_week: float):
    """
    Calculates target speed (woningen/day) either by norm percentage of total woningen or given graafsnelheid.
    Will use percentage norm for calculation if either total meters bis or graafsnelheid is not specified in project.

    Args:
        total_meters_bis: total meter graven bis in a project
        total_num_has: total number of huisaansluitingen in a project
        snelheid_m_week: graafsnelheid (meters per week) specified for a project
    Returns:

    """
    if np.isnan(total_meters_bis) or np.isnan(snelheid_m_week):
        return calculate_graafsnelheid_woning_day_by_percentage_norm(total_num_has)
    else:
        return convert_graafsnelheid_from_m_week_to_woning_day(total_meters_bis, total_num_has, snelheid_m_week)


def calculate_project_duration(snelheid_woning_day: float, total_num_has: float):
    """
    Calculates the target duration of a project based on its target graafsnelheid (woningen/day) and total number of
    woningen in the project.

    Args:
        snelheid_woning_day: target number of woningen to pass each day in a project
        total_number_has: total number of woningen in project to pass
    Returns:
        duration_days: duration of project based assuming target speed
    """
    duration_days = total_num_has / snelheid_woning_day
    return duration_days


def calculate_bis_target_of_project(civiel_startdatum: str, duration_days: float, snelheid_woning_day: float):
    """
    Calculates BIS target of a project in number of woningen to pass on each date based on target speed and duration.
    First creates series with dates based on start date and duration of porject. Next, assigns snelheid_woning_day as
    target woning to pass on date. Last, corrects the remaining work on the last day of the project.

    Args:
        civiel_startdatum: civiele startdatum of a project
        duration_days: target duration of project based assuming target speed
        snelheid_woning_day: target number of woningen to pass each day in a project
    Returns:
        target_series: A pd.Series with dates on index and woningen to pass as values
    """
    working_dates = pd.date_range(start=civiel_startdatum, periods=math.ceil(duration_days), freq='D')
    target_series = pd.Series(index=working_dates, data=snelheid_woning_day)
    target_series.iloc[-1] = (duration_days - int(duration_days)) * snelheid_woning_day
    return target_series


def sum_bis_targets_multiple_projects(civiel_startdatum: pd.Series, duration_days: pd.Series,
                                      snelheid_woning_day: pd.Series):
    """
    First, calculates BIS target of each project in number of woningen to pass on each date. Then, takes sum off BIS
    targets of all projects to determine provider level BIS target (woning/date).

    Args:
        civiel_startdatum: pd.Series of civiele startdatums of projects
        duration_days: target duration of project assuming target speed
        snelheid_woning_day: target number of woningen to pass each day in a project
    Returns:
        series_bis_target: A pd.Series with dates on index and target woningen to pass as values
    """
    # Calculate bis target of each project
    list_project_target = list(map(calculate_bis_target_of_project, civiel_startdatum, duration_days,
                                   snelheid_woning_day))

    # Sum bis targets of all projects
    series_bis_target = pd.Series()
    for series in list_project_target:
        series_bis_target = series_bis_target.add(series, fill_value=0)
    return series_bis_target


def combine_dicts_into_dataframe(civiel_startdatum: dict, total_meters_bis: dict, total_num_has: dict,
                                 snelheid_m_week: dict):
    """
    Takes project info input as dicts and merges into a dataframe.

    Args:
        civiel_startdatum: A dict with civiele startdatum for each project
        total_meters_bis: A dict with total meter graven bis for each project
        total_num_has: A dict with total number of huisaansluitingen for each project
        snelheid_m_week: A dict with graafsnelheid (meters per week) specified for each project
    Returns:
        df: Dataframe with project info
    """
    df = (pd.DataFrame.from_dict(snelheid_m_week, orient='index', columns=['snelheid_m_week']).
          merge(pd.DataFrame.from_dict(total_meters_bis, orient='index', columns=['total_meters_bis']),
                left_index=True, right_index=True, how='outer').
          merge(pd.DataFrame.from_dict(total_num_has, orient='index', columns=['total_num_has']),
                left_index=True, right_index=True, how='outer').
          merge(pd.DataFrame.from_dict(civiel_startdatum, orient='index', columns=['civiel_startdatum']),
                left_index=True, right_index=True, how='outer')
          )
    return df


def filter_projects_complete_info(df: pd.DataFrame):
    """
    Filters a dataframe with project info. Only projects with both a civiele startdatum and a value for total
    number of huisaansluitingen are returned.

    Args:
        df: A dataframe with project info
    Returns:
        filter: pd.Series: A series of truth values.
    """
    filter = ~df.civiel_startdatum.isna() & ~df.total_num_has.isna()
    return filter


def extract_bis_target_overview(civiel_startdatum: dict, total_meters_bis: dict, total_num_has: dict,
                                snelheid_m_week: dict, client: str):
    """
    Calculates BIS target in number of woningen to pass on each date. First combines and filters data. Next determines
    target speed (woning/day) and target duration (days) for each project. Then calculates target woningen per date for
    all project and takes sum of all projects.

    Args:
        civiel_startdatum: A dict with civiele startdatum for each project
        total_meters_bis: A dict with total meter graven bis for each project
        total_num_has: A dict with total number of huisaansluitingen for each project
        snelheid_m_week: A dict with graafsnelheid (meters per week) specified for each project
    Returns:
        bis_target: A pd.Series defining target bis (woning/day) for each date summed over all projects
    """
    # TODO: If statement and client parameter can be removed when DFN and TMobile provides project info
    if client in ['kpn']:
        df = combine_dicts_into_dataframe(civiel_startdatum, total_meters_bis, total_num_has, snelheid_m_week)
        df = df[filter_projects_complete_info]
        speed_graafsnelheid = convert_graafsnelheid_from_m_week_to_woning_day(df.total_meters_bis, df.total_num_has, df.snelheid_m_week)
        speed_norm = calculate_graafsnelheid_woning_day_by_percentage_norm(df.total_num_has)
        speed = speed_graafsnelheid.fillna(speed_norm)
        duration = calculate_project_duration(speed, df.total_num_has)
        bis_target = sum_bis_targets_multiple_projects(df.civiel_startdatum, duration, speed)
    else:
        bis_target = None
    return bis_target


def extract_bis_target_project(civiel_startdatum: str, total_meters_bis: float, total_num_has: float,
                               snelheid_m_week: float):
    """
    Calculates bis target series of a week.

    Args:
        civiel_startdatum: A dict with civiele startdatum for each project
        total_meters_bis: A dict with total meter graven bis for each project
        total_num_has: A dict with total number of huisaansluitingen for each project
        snelheid_m_week: A dict with graafsnelheid (meters per week) specified for each project
    Returns:
        bis_week_target: Bis target series
    """
    if isinstance(civiel_startdatum, str) & ~pd.isnull(total_num_has):
        if pd.isnull(snelheid_m_week) | pd.isnull(total_meters_bis):
            speed = calculate_graafsnelheid_woning_day_by_percentage_norm(total_num_has)
        else:
            speed = convert_graafsnelheid_from_m_week_to_woning_day(total_meters_bis, total_num_has, snelheid_m_week)
        duration = calculate_project_duration(speed, total_num_has)
        bis_target_series = calculate_bis_target_of_project(civiel_startdatum, duration, speed)
        return bis_target_series
    else:
        return pd.Series()


def ratio_sum_over_periods_to_record(numerator: pd.Series, divider: pd.Series, freq: str, year: str):
    """
    Similar to sum_over_period_to_record, but it takes two timeseries and divides them before returning the record.
    This allows for the calculation of HC/HPend ratios and <8 weeks ratios

    Args:
        numerator: A pd.Series used as numerator in division
        divider: A pd.Series used as divider in division
        freq: Either 'W-MON', 'M' or 'Y'
        year: The year to sum over

    Returns:
        Record: Record for the firestore
    """
    data_num = sum_over_period(numerator, freq, period=[year + '-01-01', year + '-12-31'])
    data_div = sum_over_period(divider, freq, period=[year + '-01-01', year + '-12-31'])
    data = (data_num / data_div).fillna(0)
    data.index = data.index.format()
    record = data.to_dict()
    return record


def voorspel_and_planning_minus_HPend_sum_over_periods_to_record(predicted: pd.Series, realized: pd.Series, freq: str,
                                                                 year: str):
    """
    Similar to sum_over_period_to_record, but it takes two timeseries and subtracts one from the other
    before returning the record. This allows for calculation of voorspelling minus HPend and planning minus HPend

    Args:
        predicted: A pd.Series to be subtracted from
        realized: A pd.Series to use for subtraction
        freq: Either 'W-MON', 'M' or 'Y'
        year: The year to sum over

    Returns:
        Record for the firestore

    """
    data_predicted = sum_over_period(predicted, freq, period=[year + '-01-01', year + '-12-31'])
    data_realized = sum_over_period(realized, freq, period=[year + '-01-01', year + '-12-31'])
    data = (data_predicted - data_realized).fillna(0)
    data.index = data.index.format()
    record = data.to_dict()
    return record


def extract_has_target_client(client, year):
    """
    Gets bis target from config for year and client. Determines has target based on defined percentage has of bis.
    Returns zero if bis target is not defined for client and year.

    Args:
        client: String of client to get target
        year: String of year to get target
    Returns:
        has_target: Int of target (woningen) agreed with client
    """
    bis_target = config.client_bis_target.get(client, {}).get(year, 0)
    has_target = int(config.perc_has_of_bis * bis_target)
    return has_target


def extract_bis_target_client(client, year):
    """
    Gets bis target from config for year and client. Returns zero if not defined.

    Args:
        client: String of client to get target
        year: String of year to get target
    Returns:
        bis_target: Int of bis target (woningen) agreed with client
    """
    bis_target = config.client_bis_target.get(client).get(year, 0)
    return bis_target


def get_timestamp_of_period(freq: str, period='next'):
    """
    This functions returns the corresponding timestamp of past, current or next week or month based a frequency

    Args:
        freq (str): frequency used to determine the time delta used to look forward or backwards.
                    With 'W-MON' the delta is a week, with 'MS' the delta is a month and with 'D' the delta is a day
        period (str): period that will be returned; Last period, current period or next period.

    Raises:
        NotImplementedError: there is no method implemented for this type of frequency.

    Returns:
        Index of chosen period (pd.Timestamp)
    """
    period_options = {}
    now = pd.Timestamp.now()

    if freq == 'D':
        period_options['last'] = pd.to_datetime(now.date() + relativedelta(days=-1))
        period_options['current'] = pd.to_datetime(now.date())
        period_options['next'] = pd.to_datetime(now.date() + relativedelta(days=1))
    elif freq == 'W-MON':
        period_options['last'] = pd.to_datetime(now.date() + relativedelta(days=-7 - now.weekday()))
        period_options['current'] = pd.to_datetime(now.date() - relativedelta(days=now.weekday()))
        period_options['next'] = pd.to_datetime(now.date() + relativedelta(days=7 - now.weekday()))
    elif freq == 'MS':
        period_options['last'] = pd.Timestamp(now.year, now.month, 1) + relativedelta(months=-1)
        period_options['current'] = pd.Timestamp(now.year, now.month, 1)
        period_options['next'] = pd.Timestamp(now.year, now.month, 1) + relativedelta(months=1)
    else:
        raise NotImplementedError('There is no output period implemented for this frequency {}'.format(freq))

    period_timestamp = period_options.get(period)
    if period_timestamp:
        return period_timestamp
    else:
        raise NotImplementedError(f'The selected period "{period}" '
                                  'is not valid. Choose "last", "current" or "next"')
