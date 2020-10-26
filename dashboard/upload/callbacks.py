import datetime

import dash_html_components as html
from dash.dependencies import Input, Output, State

from app import app
from config import upload_config
from upload.Validators import *  # noqa: F403, F401
from upload.Validators import ValidationError


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
                return [f"Verzenden... {upload_config[validator]}"]
        except ValidationError as e:
            return [str(e)]
    return [f"{validator}"]
