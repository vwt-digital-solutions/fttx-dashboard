from data import api, data
from urllib import parse
import logging


def get_document(collection, **url_params):
    url = f"/{collection}?{parse.urlencode(url_params)}"
    result = api.get(url)
    if not result or not len(result):
        logging.warning(f"Query {url} did not return any results.")
        return {}
    if len(result) > 1:
        logging.warning(f"Query {url} resulted in {len(result)} results, only the first is returned")
    return result[0].get('record', 'n.v.t.')


def get_graph(**kwargs):
    return get_document(collection="Graphs", **kwargs).get('figure', data.no_graph())
