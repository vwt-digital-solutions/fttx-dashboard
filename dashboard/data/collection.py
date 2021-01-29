from data import api, data
from urllib import parse
import logging


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
        logging.warning(f"Query {url} did not return any results.")
        return {}
    if len(result) > 1:
        logging.warning(f"Query {url} resulted in {len(result)} results, only the first is returned")
    return result[0].get('record', 'n.v.t.')


def get_graph(**filters):
    """
    This function retrieves the graph from the firestore. The graph is in the record field of a document in the Graphs
    collection. This function automatically retrieves the necessary data from the document.

    Args:
        **filters: See :func:`get_document`

    Returns:
        dict: The graph in the document
    """
    return get_document(collection="Graphs", **filters).get('figure', data.no_graph())
