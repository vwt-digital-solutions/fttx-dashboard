import logging
import os
from io import BytesIO
from datetime import datetime

import dash
import dash_bootstrap_components as dbc
import flask
import pandas as pd
from flask import send_file
from flask_caching import Cache
from flask_cors import CORS
from flask_sslify import SSLify
from sqlalchemy import create_engine

import config
import utils
from authentication.azure_auth import AzureOAuth

from werkzeug.middleware.proxy_fix import ProxyFix

from toggles import ReleaseToggles

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
    server=server
)

app.css.append_css(
    {'external_url': 'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css'}
)

cache = Cache(app.server,
              config={'CACHE_TYPE': 'simple'}
              )

logging.info("Setting serve locally to false")
app.css.config.serve_locally = False
app.scripts.config.serve_locally = False

logging.info("Setting toggles")
toggles = ReleaseToggles('toggles.yaml')

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


def get_database_engine():
    if 'db_ip' in config.database:
        SACN = 'mysql+mysqlconnector://{}:{}@{}:3306/{}?charset=utf8&ssl_ca={}&ssl_cert={}&ssl_key={}'.format(
            config.database['db_user'],
            utils.get_secret(config.database['project_id'], config.database['secret_name']),
            config.database['db_ip'],
            config.database['db_name'],
            config.database['server_ca'],
            config.database['client_ca'],
            config.database['client_key']
        )
    else:
        SACN = 'mysql+pymysql://{}:{}@/{}?unix_socket=/cloudsql/{}:europe-west1:{}'.format(
            config.database['db_user'],
            utils.get_secret(config.database['project_id'], config.database['secret_name']),
            config.database['db_name'],
            config.database['project_id'],
            config.database['instance_id']
        )

    return create_engine(SACN, pool_recycle=3600)


def download_from_sql(query):
    sqlEngine = get_database_engine()
    try:
        result = pd.read_sql(query, sqlEngine)
    except ValueError as e:
        logging.info(e)
        result = pd.DataFrame()
    return result


def df_to_excel(df: pd.DataFrame, relevant_columns: list = None):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    if df.empty:
        result = pd.DataFrame(columns=relevant_columns)
        result.to_excel(writer, index=False)
    else:
        if relevant_columns:
            df[relevant_columns].to_excel(writer, index=False)
        else:
            df.to_excel(writer, index=False)
    writer.save()
    output.seek(0)
    return output


@app.server.route('/dash/order_wait_download')
def order_wait_download():
    from data.download_queries import waiting_category

    wait_category = flask.request.args.get('wait_category')
    order_type = flask.request.args.get('order_type')
    project = flask.request.args.get('project')
    logging.info(f"Collecting data for {wait_category}, {order_type}.")

    result = download_from_sql(waiting_category(project=project, wait_category=wait_category, order_type=order_type))

    relevant_columns = ['adres',
                        'postcode',
                        'huisnummer',
                        'soort_bouw',
                        'toestemming',
                        'creationdatum',
                        'opleverstatus',
                        'opleverdatum',
                        'hasdatum',
                        'cluster_redenna',
                        'redenna',
                        'toelichting_status',
                        'plan_type',
                        'wachttijd'
                        ]

    excel = df_to_excel(result, relevant_columns)
    now = datetime.now().strftime('%Y%m%d')

    return send_file(excel,
                     mimetype='application/vnd.ms-excel',
                     attachment_filename=f'{now}_{project}_{wait_category}.xlsx',
                     as_attachment=True)


@app.server.route('/dash/project_redenna_download')
def project_redenna_download():
    from data.download_queries import project_redenna
    args = {key.lower(): value for key, value in flask.request.args.items()}
    project = args.pop('project')

    query = project_redenna(project, **args)

    result = download_from_sql(query)

    relevant_columns = ["sleutel",
                        "project",
                        "plaats",
                        "postcode",
                        "adres",
                        "huisnummer",
                        'cluster_redenna',
                        'redenna',
                        'toelichting_status',
                        'soort_bouw',
                        'schouwdatum',
                        "opleverstatus",
                        "opleverdatum",
                        'laswerkdpgereed',
                        'laswerkapgereed',
                        'hasdatum',
                        'creationdatum']  # TODO: only add creationdatum when client == tmobile

    excel = df_to_excel(result, relevant_columns)
    now = datetime.now().strftime('%Y%m%d')

    arg_string = "-".join(f"{key}_{value}" for key, value in args.items())
    return send_file(excel,
                     mimetype='application/vnd.ms-excel',
                     attachment_filename=f'{now}_{project}_redenna-{arg_string}.xlsx',
                     as_attachment=True)
