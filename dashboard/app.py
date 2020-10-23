import logging
import os
from io import BytesIO

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

logging.info("creating flask server")
server = flask.Flask(__name__)

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

    relevant_columns = ['HAS_status',
                        'aannemer',
                        'adres',
                        'bis_status',
                        'cluster_redenna',
                        'hasdatum',
                        'hpend',
                        'huisext',
                        'huisnummer',
                        'in_has_werkvoorraad',
                        'laagbouw',
                        'lasAP_status',
                        'lasDP_status',
                        'opgeleverd',
                        'opleverdatum',
                        'opleverstatus',
                        'oplevertijd',
                        'ordered',
                        'plaats',
                        'postcode',
                        'project',
                        'projectleider',
                        'redenna',
                        'schouw_status',
                        'schouwakkoord',
                        'schouwdatum',
                        'sleutel',
                        'sleuteloverdracht',
                        'soort_bouw',
                        'sor_aanwezig',
                        'status_civiel',
                        'status_civiel_datum',
                        'team',
                        'toelichting_status',
                        'toestemming',
                        'toestemming_datum',
                        'voorkeur',
                        'wait_category',
                        ]

    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    result[relevant_columns].to_excel(writer)
    writer.save()
    output.seek(0)

    return send_file(output,
                     mimetype='application/vnd.ms-excel',
                     attachment_filename='downloadFile.xlsx',
                     as_attachment=True)
