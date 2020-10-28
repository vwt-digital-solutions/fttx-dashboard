import datetime

import dash_html_components as html
import requests
from dash.dependencies import Input, Output, State
from flask_dance.contrib.azure import azure

from app import app
from config import upload_config, upload_url
from upload.Validators import *  # noqa: F403, F401
from upload.Validators import ValidationError
import logging

logger = logging.getLogger("Upload Callbacks")


@app.callback(Output('output-data-upload', 'children'),
              [Input('upload-data', 'contents')],
              [State('upload-data', 'filename'),
               State('upload-data', 'last_modified')])
def update_output(content, name, date):
    if content is not None:
        children = [
            html.H5(name),
            html.H6(datetime.datetime.fromtimestamp(date)),
        ]
        return children


@app.callback(Output('submit-result', 'children'),
              [Input('upload-submit', 'n_clicks')],
              [
                  State('upload-validator', 'value'),
                  State('upload-data', 'contents'),
                  State('upload-data', 'filename'),
                  State('upload-data', 'last_modified')
              ])
def submit_files(n_clicks, validator, content, name, date):
    if not validator:
        return ["Geen upload type geselecteerd."]
    if n_clicks and content is not None:
        validator_class = globals()[upload_config[validator]['validator']](file_content=content,
                                                                           file_name=name,
                                                                           modified_date=date,
                                                                           **upload_config[validator])
        try:
            if validator_class.validate():
                try:
                    r = send_file(content, name)
                except Exception as e:
                    return [f"Sending failed {e}"]
                return [f"Verzenden... {upload_config[validator]}, {r}"]
        except ValidationError as e:
            return [str(e)]
    return [f"{validator}"]


def send_file(file_content, file_name):
    url = upload_url + "/test"
    logger.info(url)
    headers = {'Authorization': 'Bearer ' + azure.access_token,
               'Content-Type': 'application/vnd.ms-excel'}
    data = {'name': file_name}
    files = {'file': (file_name, file_content, 'application/vnd.ms-excel', {'Expires': '0'})}
    r = requests.post(url, files=files, headers=headers, data=data)
    logger.info(r)
    return r
