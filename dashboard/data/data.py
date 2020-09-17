from datetime import datetime
from data import collection
import pandas as pd


def has_planning_by(period, provider):
    has_opgeleverd = collection.get_document(collection="Data", id="count_opleverdatum_by_" + period, client=provider)
    has_planning = collection.get_document(collection="Data", id="count_hasdatum_by_" + period, client=provider)
    has_outlook = collection.get_document(collection="Data", id="count_outlookdatum_by_" + period, client=provider)
    # temporary solution until we also have outlook data for T-Mobile
    if not has_outlook:
        has_outlook['count_outlookdatum'] = has_opgeleverd['count_opleverdatum'].copy()
        for el in has_outlook['count_outlookdatum']:
            has_outlook['count_outlookdatum'][el] = 0
        if period == 'month':
            has_outlook['count_outlookdatum']['2020-11-30'] = 0
            has_outlook['count_outlookdatum']['2020-12-31'] = 0
    has_voorspeld = collection.get_document(collection="Data", id="count_voorspellingdatum_by_" + period)
    # temporary solution until we also have voorspelling data for T-Mobile
    if not has_voorspeld:
        has_voorspeld['count_voorspellingdatum'] = has_opgeleverd['count_opleverdatum'].copy()
        for el in has_voorspeld['count_voorspellingdatum']:
            has_voorspeld['count_voorspellingdatum'][el] = 0

    df = pd.DataFrame({**has_planning, **has_opgeleverd, **has_outlook,
                       **has_voorspeld}).reset_index().fillna(0).rename(columns={"index": "date"})
    df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d")
    df['period'] = period
    df['provider'] = provider

    start_date = pd.to_datetime(str(datetime.now().year) + '-01-01', format="%Y-%m-%d")
    end_date = pd.to_datetime(str(datetime.now().year) + '-12-31', format="%Y-%m-%d")
    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    return df[mask]
