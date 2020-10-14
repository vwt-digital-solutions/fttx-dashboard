import importlib
import logging
import dash_html_components as html
from urllib import parse
from layout.components.nav_bar import nav_bar


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


def get_layout(pathname="/", brand=""):
    client, _, remainder = pathname.lstrip("/").partition("/")
    print(client, remainder)
    if not client:
        client = "main_page"
    page_body = get_page(client)

    layout = html.Div(
        [
            nav_bar(pathname, brand),
            html.Div(page_body.get_body(client, parse.unquote_plus(remainder)))
        ]
    )
    return layout
