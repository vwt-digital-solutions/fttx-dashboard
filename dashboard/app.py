import logging
import os
from io import BytesIO
from datetime import datetime

import dash
import dash_bootstrap_components as dbc
import flask
import pandas as pd
from flask import send_file
from flask_cors import CORS
from flask_sslify import SSLify

import config
import utils
from authentication.azure_auth import AzureOAuth
from data import api
from werkzeug.middleware.proxy_fix import ProxyFix

logging.info("creating flask server")
server = flask.Flask(__name__)
server.wsgi_app = ProxyFix(server.wsgi_app, x_for=1, x_host=1)

logging.info("Setting CORS")
if 'GAE_INSTANCE' in os.environ:
    SSLify(server, permanent=True)
    CORS(server, origins=config.ORIGINS)
else:
    CORS(server)

logging.info("Creating Dash App")
app = dash.Dash(
    __name__,
    meta_tags=[{"name": "viewport", "content": "width=device-width"}],
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    server=server,
)

logging.info("Setting serve locally to false")
app.css.config.serve_locally = False
app.scripts.config.serve_locally = False

logging.info("supressing call back exceptions")
app.config.suppress_callback_exceptions = True

app.title = "FttX operationeel"

# Azure AD authentication
if config.authentication:
    session_secret = utils.get_secret(
        config.authentication['gcp_project'],
        config.authentication['secret_name'])

    auth = AzureOAuth(
        app,
        config.authentication['client_id'],
        config.authentication['client_secret'],
        config.authentication['expected_issuer'],
        config.authentication['expected_audience'],
        config.authentication['jwks_url'],
        config.authentication['tenant'],
        session_secret,
        config.authentication['role'],
        config.authentication['required_scopes']
    )
    logging.info("Authorization is set up")


@app.server.route('/dash/order_wait_download')
def download_csv():
    wait_category = flask.request.args.get('wait_category')
    project = flask.request.args.get('project')
    logging.info(f"Collecting data for {wait_category}.")

    request_result = api.get(f"/Houses/?record.wait_category={wait_category}&record.project={project}")

    result = pd.DataFrame(
        x['record'] for x in request_result)

    relevant_columns = [
        'adres',
        'postcode',
        'huisnummer',
        'soort_bouw',
        'toestemming_datum',
        'opleverstatus',
        'schouw_status',
        'hasdatum',
        'cluster_redenna',
        'redenna',
        'toelichting_status',
        'voorkeur'
    ]

    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    if result.empty:
        result = pd.DataFrame(columns=relevant_columns)
        result.to_excel(writer, index=False)
    else:
        result[relevant_columns].to_excel(writer, index=False)
    writer.save()
    output.seek(0)
    now = datetime.now().strftime('%Y%m%d')
    return send_file(output,
                     mimetype='application/vnd.ms-excel',
                     attachment_filename=f'{now}_{project}_{wait_category}.xlsx',
                     as_attachment=True)
