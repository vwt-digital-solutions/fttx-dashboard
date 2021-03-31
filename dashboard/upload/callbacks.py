import datetime

import dash
import dash_html_components as html
import requests
from dash.dependencies import Input, Output, State
from flask_dance.contrib.azure import azure

from app import app
from config import upload_config, upload_url
from upload.Validators import *  # noqa: F403, F401
from upload.Validators import ValidationError, Validator

import json
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


@app.callback(
    Output('submit-result', 'children'),
    [
        Input('upload-submit', 'n_clicks'),
        Input("upload_modal-close", "n_clicks")
    ],
    [
        State('upload-validator', 'value'),
        State('upload-data', 'contents'),
        State('upload-data', 'filename'),
        State('upload-data', 'last_modified')
    ]
)
def submit_files(n_clicks, close_clicks, validator, content, name, date):
    ctx = dash.callback_context

    if not any(ctx.inputs.values()) or 'upload_modal-close.n_clicks' in [prop['prop_id'] for prop in ctx.triggered]:
        return []

    if not validator:
        return ["Geen upload type geselecteerd."]
    if not content:
        return ["Geen bestand geselecteerd."]
    if n_clicks and content is not None:
        validator_class: Validator = globals()[upload_config[validator]['validator']](file_content=content,
                                                                                      file_name=name,
                                                                                      modified_date=date,
                                                                                      **upload_config[validator])
        try:
            if validator_class.validate():
                try:
                    send_file(file_content=validator_class.file_content,
                              content_type=validator_class.content_type,
                              path=upload_config[validator]['path'])
                except Exception as e:
                    logger.warning(f"Sending a file failed: {e}")
                    return ["Het versturen van het bestand is mislukt"]
                return ["✔️"]
        except ValidationError as e:
            return [f"❌ {e}"]
    return [f"{validator}"]


def send_file(file_content, content_type, path):
    url = upload_url + path
    logger.info(url)
    headers = {'Authorization': 'Bearer ' + azure.access_token,
               'Content-Type': content_type}
    r = requests.post(url, data=file_content, headers=headers)
    if r.status_code != 201:
        message = f"❌ Status: {r.status_code}"
        if "json" in r.headers.get("Content-Type"):
            message += json.loads(r.content).get('detail')
        raise RuntimeError(message)
