from datetime import datetime
from collections import namedtuple

from data import collection
import pandas as pd


def has_planning_by(period, client):
    has_opgeleverd = collection.get_document(collection="Data", graph_name="count_opleverdatum_by_" + period, client=client)
    has_planning = collection.get_document(collection="Data", graph_name="count_hasdatum_by_" + period, client=client)
    has_outlook = collection.get_document(collection="Data", graph_name="count_outlookdatum_by_" + period, client=client)
    has_voorspeld = collection.get_document(collection="Data", graph_name="count_voorspellingdatum_by_" + period, client=client)
    # temporary solution until we have outlook data for T-Mobile
    if not has_outlook:
        has_outlook['count_outlookdatum'] = has_opgeleverd['count_opleverdatum'].copy()
        for el in has_outlook['count_outlookdatum']:
            has_outlook['count_outlookdatum'][el] = 0
        if period == 'month':
            has_outlook['count_outlookdatum']['2020-11-02'] = 0
            has_outlook['count_outlookdatum']['2020-12-01'] = 0
    # temporary solution until we also have voorspelling data for T-Mobile
    if not has_voorspeld:
        has_voorspeld['count_voorspellingdatum'] = has_opgeleverd['count_opleverdatum'].copy()
        for el in has_voorspeld['count_voorspellingdatum'].keys():
            has_voorspeld['count_voorspellingdatum'][el] = 0

    df = pd.DataFrame({**has_planning, **has_opgeleverd, **has_outlook,
                       **has_voorspeld}).reset_index().fillna(0).rename(columns={"index": "date"})
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
    df['period'] = period
    start_date = pd.to_datetime(str(datetime.now().year) + '-01-01', format="%Y-%m-%d")
    end_date = pd.to_datetime(str(datetime.now().year) + '-12-31', format="%Y-%m-%d")
    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    return df[mask]


def completed_status_counts(project_name, click_filter=None):
    StatusCountDataFrames = namedtuple("StatusCountDataFrames",
                                       ["laagbouw", "hoogbouw"])  # Used to return a named tuple

    if not click_filter:
        click_filter = {}
    categories = ["geschouwd",
                  "bis_gereed",
                  "lasDP",
                  "lasAP",
                  "HAS"]

    if project_name:
        counts = pd.DataFrame(collection.get_document(collection="Data",
                                                      graph_name="completed_status_counts",
                                                      project=project_name))

        mask = pd.Series([True]).repeat(len(counts.index)).values
        if click_filter:
            for col, value in click_filter.items():
                mask = mask & (counts[col] == value)

        laagbouw_matrix = []
        hoogbouw_matrix = []

        for category in categories:
            cols = list(dict.fromkeys(list(click_filter.keys()) + [category]))
            cols.append("laagbouw")
            cols_to_see = cols + ["count"]
            result = counts[mask][cols_to_see].groupby(cols).sum().reset_index()
            lb = result[result.laagbouw][[category, "count"]]
            hb = result[~result.laagbouw][[category, "count"]]
            for i, row in lb.iterrows():
                laagbouw_matrix.append([category] + row.values.tolist())
            for i, row in hb.iterrows():
                hoogbouw_matrix.append([category] + row.values.tolist())

        lb_df = pd.DataFrame(laagbouw_matrix, columns=["phase", "status", "count"])
        hb_df = pd.DataFrame(hoogbouw_matrix, columns=["phase", "status", "count"])

        status_category = pd.CategoricalDtype(categories=['niet_opgeleverd', "opgeleverd", "opgeleverd_zonder_hc"])
        phase_category = pd.CategoricalDtype(categories=['geschouwd', 'bis_gereed', 'lasAP', 'lasDP', 'HAS'])
        lb_df['status'] = lb_df.status.astype(status_category)
        lb_df['phase'] = lb_df.phase.astype(phase_category)
        lb_df = lb_df.groupby(by=['phase', 'status']).sum().reset_index()
        lb_df['count'] = lb_df['count'].fillna(0)
        hb_df['status'] = hb_df.status.astype(status_category)
        hb_df['phase'] = hb_df.phase.astype(phase_category)
        hb_df = hb_df.groupby(by=['phase', 'status']).sum().reset_index()
        hb_df['count'] = hb_df['count'].fillna(0)

        return StatusCountDataFrames(lb_df, hb_df)

    return StatusCountDataFrames(pd.DataFrame(columns=["phase", "status", "count"]),
                                 pd.DataFrame(columns=["phase", "status", "count"]))
