import logging

import config
import utils
from authentication.azure_auth import AzureOAuth

logging.basicConfig(
    format=' %(asctime)s - %(name)s -%(levelname)s - %(filename)s:%(funcName)s:%(lineno)s - %(message)s',
    level=logging.INFO)

from layout.components import modal  # noqa: E402
from upload import component as upload_component  # noqa: E402

logging.info("Importing Dash components")
import dash_core_components as dcc  # noqa: E402
import dash_html_components as html  # noqa: E402
from dash.dependencies import Input, Output  # noqa: E402

logging.info("Imporing callbacks")
from callbacks import *  # noqa: F403, F401, E402

logging.info("Importing the rest")
from layout import default  # noqa: E402
import importlib  # noqa: E402
logging.info("Importing Done")


def get_page(page):
    logging.info(f"Getting page {page}")
    try:
        page_body = importlib.import_module(f"layout.pages.{page}.{page}")
    except ImportError:
        page_body = importlib.import_module(f"layout.pages.{page}")
    logging.info("Returning page")
    return page_body


if __name__ == "__main__":
    logging.info("Importing App")
    from app import app, toggles  # noqa: E402

    if toggles.upload:
        from upload import callbacks  # noqa: F403, F401, E402

    logging.info("Setting base layout")
    app.layout = html.Div([
        dcc.Location(id='url', refresh=True),
        html.Div(id='page-content'),
        modal.create_modal(modal_body=upload_component.get_html(),
                           modal_id="upload_modal",
                           input_id="upload_button",
                           modal_title="Upload")
    ])

    # CALLBACKS
    @app.callback(
        Output('page-content', 'children'),
        [
            Input('url', 'pathname')
        ]
    )
    def display_page(pathname):
        layout = default.get_layout
        return [layout(pathname=pathname, brand="FttX")]

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

    logging.info("Starting server")
    app.run_server(debug=True)
