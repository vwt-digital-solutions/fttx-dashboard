import config
import requests

from flask_dance.contrib.azure import azure


def get(path):
    headers = {'Authorization': 'Bearer ' + azure.access_token}
    url = config.api_url + path
    response = requests.get(url, headers=headers)

    return response.json().get('results')
