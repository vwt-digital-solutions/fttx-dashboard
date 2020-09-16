from collections import namedtuple
from datetime import datetime, timedelta

from data import collection
import pandas as pd


def has_planning_by_week(days_before=30, days_after=30):
    has_done = collection.get_document(collection="Data", id="count_opleverdatum_by_week")
    has_planning = collection.get_document(collection="Data", id="count_hasdatum_by_week")
    df = pd.DataFrame({**has_planning, **has_done}).reset_index().fillna(0).rename(columns={"index": "date"})
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
    start_date = datetime.now() - timedelta(days=days_before)
    end_date = datetime.now() + timedelta(days=days_after)
    mask = (df['date'] > start_date) & (df['date'] <= end_date)
    return df[mask]


def has_planning_by_month():
    has_done = collection.get_document(collection="Data", id="count_opleverdatum_by_month")
    has_planning = collection.get_document(collection="Data", id="count_hasdatum_by_month")
    df = pd.DataFrame({**has_planning, **has_done}).reset_index().fillna(0).rename(columns={"index": "date"})
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
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
