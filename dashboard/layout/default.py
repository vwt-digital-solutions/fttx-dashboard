import importlib
import logging
import dash_html_components as html
from urllib import parse

from layout.components import overview
from layout.components.header import header
from layout.components.nav_bar import nav_bar
from layout.pages import error, main_page
from utils import get_client_name


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


def client_page_body(client, project):
    project_view = importlib.import_module(f"layout.pages.{client}.project_view")
    body = [
        header(f"Status projecten {get_client_name(client)} in 2020"),
        html.Div(
            overview.get_search_bar(client, project),
            className="container-display",
            id="title",
        ),
        html.Div(
            id=f"{client}-overview",
            children=overview.get_html(client),
        ),
        html.Div(
            style={'display': 'none'},
            id=f"{client}-project-view",
            children=project_view.get_html(client),
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
