# import logging
from urllib import parse

import pandas as pd

from data import api, data


def get_document(collection, **filters):
    """
    This function returns a the record field from a document in the firestore. It returns never more than one document.
    When multiple documents are returned by the firestore, only the first one is returned.

    For example: :code:`get_document(collection, client='kpn', project='my_project', graph_name='my_graph')`

    Args:
        collection (str): The firestore collection where the documents are located.
        **filters: Each argument is a field in the document and the value of the argument is the required value of the
            field in the document.


    Returns:
        dict: The record in the document
    """
    url = f"/{collection}?{parse.urlencode(filters)}"
    result = api.get(url)
    if not result or not len(result):
        # logging.warning(f"Query {url} did not return any results.")
        return {}
    # if len(result) > 1:
    # logging.warning(
    # f"Query {url} resulted in {len(result)} results, only the first is returned"
    # )
    return result[0].get("record", "n.v.t.")


def get_graph(**filters):
    """
    This function retrieves the graph from the firestore. The graph is in the record field of a document in the Graphs
    collection. This function automatically retrieves the necessary data from the document.

    Args:
        **filters: See :func:`get_document`

    Returns:
        dict: The graph in the document
    """
    return get_document(collection="Graphs", **filters).get("figure", data.no_graph())


def get_year_value_from_document(collection, year, **filters):
    doc = get_document(collection, **filters)
    if doc:
        value = doc["series_year"][year + "-01-01"]
    else:
        value = "n.v.t."
    return value


def get_current_week_value_from_document(collection, **filters):
    doc = get_document(collection, **filters)
    if doc:
        value = doc["current_week"]
    else:
        value = 0
    return value


def get_month_series_from_document(collection, year, **filters):
    doc = get_document(collection, **filters)
    if doc:
        series = pd.Series(doc["series_month_" + year])
        series.index = pd.to_datetime(series.index)
        series = pd.Series(
            index=pd.date_range(start=year, periods=12, freq="MS"), data=0
        ).add(series, fill_value=0)
    else:
        series = pd.Series()
    return series


def get_week_series_from_document(collection, year, **filters):
    doc = get_document(collection, **filters)
    if doc:
        series = pd.Series(doc["series_week_" + year])
        series.index = pd.to_datetime(series.index)
        series = pd.Series(
            index=pd.date_range(start=year, periods=52, freq="W-MON"), data=0
        ).add(series, fill_value=0)
    else:
        series = pd.Series()
    return series


def get_cumulative_week_series_from_document(collection, **filters):
    doc = get_document(collection, **filters)
    if doc:
        series = pd.Series(doc["series_week"]).cumsum()
    else:
        series = pd.Series()
    return series


def get_redenna_overview_from_document(collection, date, period, **filters):
    cluster_types = [
        "HC",
        "geplande aansluiting",
        "permissieobstructies",
        "technische obstructies",
    ]
    series_type = "series_" + period
    pie_chart_dict = {}
    for cluster in cluster_types:
        filters["line"] = "RedenNAindicator_" + cluster
        value = get_document(collection, **filters)[series_type][date]
        pie_chart_dict[cluster] = value if value else 0
    return {date: pie_chart_dict}
