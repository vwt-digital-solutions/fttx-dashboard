from data import api
from urllib import parse
import logging


def get_document(collection, **url_params):
    # collection_result_key = {
    #     "Graphs": "figure",
    #     "Data": "record"
    # }

    url = f"/{collection}?{parse.urlencode(url_params)}"
    result = api.get(url)
    if not result or not len(result):
        logging.warning(f"Query {url} did not return any results.")
        return {}
    if len(result) > 1:
        logging.warning(f"Query {url} resulted in {len(result)} results, only the first is returned")
    return result[0]['record']


def get_graph(**kwargs):
    return get_document(**kwargs).get('figure', {
            'data': [
                {'x': [1, 2, 3], 'y': [4, 1, 2]},
            ],
            'layout': {
                'title': 'Graph not found'
            }
        })
