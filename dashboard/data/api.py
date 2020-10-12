import config
import requests

from flask_dance.contrib.azure import azure
import logging


def get(path):
    logging.info(f"Requesting {path}")
    headers = {'Authorization': 'Bearer ' + azure.access_token}
    url = config.api_url + path
    response = requests.get(url, headers=headers)

    return response.json().get('results')
