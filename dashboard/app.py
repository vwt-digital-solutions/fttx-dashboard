import os
from flask import send_file
from io import BytesIO
import utils
import dash
import flask
import config
import dash_bootstrap_components as dbc

from authentication.azure_auth import AzureOAuth
from flask_sslify import SSLify
from flask_cors import CORS
import logging

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


@app.server.route('/dash/urlToDownload')
def download_csv():
    import pandas as pd

    data1 = [
        [1, 2, 3],
        [4, 5, 6]
    ]
    df = pd.DataFrame(data1, columns=['col1', 'col2', 'col3'])

    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer)
    writer.save()
    output.seek(0)

    return send_file(output,
                     mimetype='application/vnd.ms-excel',
                     attachment_filename='downloadFile.xlsx',
                     as_attachment=True)
