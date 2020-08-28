from datetime import datetime, timedelta

from data import collection
import pandas as pd


def has_planning_by_week(days_before=30, days_after=30):
    has_done = collection.get_document(collection="Data", id="count_hasdatum_by_week")
    has_planning = collection.get_document(collection="Data", id="count_plandatum_by_week")
    df = pd.DataFrame({**has_planning, **has_done}).reset_index().fillna(0).rename(columns={"index": "date"})
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
    start_date = datetime.now() - timedelta(days=days_before)
    end_date = datetime.now() + timedelta(days=days_after)
    mask = (df['date'] > start_date) & (df['date'] <= end_date)
    return df[mask]
