import os
from urllib.parse import urlparse, parse_qs

from flask import make_response, jsonify
from google.cloud import firestore_v1

import config
import requests

from flask_dance.contrib.azure import azure
import logging

from app import cache


def get(path):
    url = config.api_url + path
    if 'FIRESTORE_EMULATOR_HOST' in os.environ:
        return local_api(url)
    else:
        headers = {'Authorization': 'Bearer ' + azure.access_token}
        response = cachable_request(url, headers)
        if response.status_code == 404:
            logging.info(f"Path {path} not found: 404")
        return response.json().get('results')


@cache.memoize(timeout=60*10)
def cachable_request(url, headers):
    logging.info(f"Requesting {url}")
    response = requests.get(url, headers=headers)
    return response


def make_problem_json(title, status):
    return make_response(jsonify({'title': title, 'status': status}), status)


def catch_error(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.exception(f'Exception occurred: {e}')
            return make_problem_json('Internal error', 500)

    return wrapper


@catch_error
def local_api(url):
    """Returns data from a firestore query."""
    parsed_url = urlparse(url)
    query = {key: value[0] for key, value in parse_qs(parsed_url.query).items()}
    path = parsed_url.path
    collection = path.split('/')[1]

    db = firestore_v1.Client()
    q = db.collection(collection)

    max = int(os.getenv('MAX', 3000))
    page_size = int(query.pop('page_size', max))
    if page_size > max:
        page_size = max
    q = q.limit(page_size)

    if query.get('next_cursor'):
        id = query.pop('next_cursor')
        snapshot = db.collection(collection).document(id).get()
        logging.info(f'Starting query at cursor: {id}')
        if snapshot:
            q = q.start_after(snapshot)

    # Return filtered documents
    for field, value in query.items():
        logging.info(f'Filtering {field} == {value}')
        q = q.where(field, '==', value)

    docs = q.stream()
    results = []
    for doc in docs:
        results.append(doc.to_dict())

    next = ''
    size = len(results)
    if results and (page_size == size):
        next = f'/{collection}?next_cursor={doc.id}&page_size={page_size}'
        for field, value in query.items():
            next = next + f'&{field}={value}'

    logging.info(f'Returning {size} record(s)!')

    return results
