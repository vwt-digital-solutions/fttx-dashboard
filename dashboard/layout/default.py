import importlib
import logging
import dash_html_components as html
from urllib import parse

from layout.components import overview
from layout.components.header import header
from layout.components.nav_bar import nav_bar
from layout.pages import error, main_page
from utils import get_client_name
import dash_bootstrap_components as dbc


def get_page(page):
    logging.info(f"Getting page {page}")
    try:
        import_string = f"layout.pages.{page}.{page}"
        page_body = importlib.import_module(import_string)
        logging.info(f'Imported {import_string}')
    except ImportError:
        page_body = importlib.import_module(f"layout.pages.{page}")
    logging.info("Returning page")
    return page_body


def client_tabbed_view(view1, view2, name1="Tab 1", name2="Tab 2"):
    tabs = dbc.Tabs(
        [
            dbc.Tab(view1, label=name1),
            dbc.Tab(view2, label=name2)
        ],
        className=" mt-5 mb-3"
    )

    return tabs


def client_project_view(client):
    try:
        operational_view = importlib.import_module(f"layout.pages.{client}.project_operational_view").get_html(client)
    except (ImportError, AttributeError):
        operational_view = None

    try:
        financial_view = importlib.import_module(f"layout.pages.{client}.project_financial_view").get_html(client)
    except (ImportError, AttributeError):
        financial_view = None

    if operational_view and financial_view:
        return client_tabbed_view(operational_view, financial_view, name1="Operationeel", name2="Financieel")
    elif operational_view:
        return operational_view
    elif financial_view:
        return financial_view

    old_view = importlib.import_module(f"layout.pages.{client}.project_view").get_html(client)
    return old_view


def client_page_body(client, project):
    body = [
        header(f"Status projecten {get_client_name(client)}"),
        html.Div(
            overview.get_search_bar(client, project),
            className="container-display",
            id="title",
        ),
        html.Div(
            id=f"{client}-overview",
            children=overview.get_html(client),
            style={'display': "block"}
        ),
        html.Div(
            style={'display': 'none'},
            id=f"{client}-project-view",
            children=client_project_view(client),
        )
    ]
    return body


def get_page_body(client, project):
    if client:
        try:
            body = client_page_body(client, project)
        except ImportError:
            body = error.get_body()
    else:
        body = main_page.get_body()

    return html.Div(
        children=body,
        id="mainContainer",
        style={"display": "flex", "flex-direction": "column"},
    )


def get_layout(pathname="/", brand=""):
    client, _, remainder = pathname.lstrip("/").partition("/")
    print(client, remainder)
    project = parse.unquote_plus(remainder)
    page_body = get_page_body(client, project)

    layout = html.Div(
        [
            nav_bar(client, brand),
            html.Div(page_body)
        ]
    )
    return layout
