import importlib
import os

import dash
import dash_html_components as html
from urllib import parse

from layout.components import overview
from layout.components.header import header
from layout.components.nav_bar import nav_bar
from layout.pages import error, main_page
from utils import get_client_name
import dash_bootstrap_components as dbc


def client_tabbed_view(views):
    """
    Create a tabbed view for a given list of views. The list

    Args:
        views (list): A list that contains dictionaries with keys for the view and the
            tab name.

            ::

                [
                    {'view': dash.development.base_component.Component(),
                     'tab_name': 'Tab name'}
                ]

    Returns:

    """
    tabs = dbc.Tabs(
        [
            dbc.Tab(view['view'], label=view['tab_name'])
            for view in views
        ],
        className=" mt-5 mb-3"
    )

    return tabs


def client_project_view(client) -> dash.development.base_component.Component:
    """
    Construct the project specific view for a client. When multiple views are available the views are displayed in tabs.
    When there is only one view, the view is displayed directly.

    Args:
        client (str): The client for which the view is created.

    Returns:
        dash.development.base_component.Component: A dash component of the view.
    """
    tabs = {f"layout.pages.{client}.{file.rstrip('.py')}"
            for file in os.listdir(f"layout/pages/{client}")
            if file.endswith(".py")
            }

    views = []
    for tab in tabs:
        views.append(get_tab_view(client, tab))

    views = sorted(views, key=lambda view_data: int(view_data.get("tab_order", 100)))

    if len(views) > 1:
        return client_tabbed_view(views)
    return views[0]['view']


def get_tab_view(client: str, tab: str) -> dict:
    """
    Import the view and populate a dict with the view, the name and the order. The name and order are retrieved from the
    docstring of the view.


    Args:
        client (str): The client name.
        tab (str): The view to be imported.

    Returns:
        dict: A dictionary with the following keys: view, tab_name and tab_order.
    """
    module = importlib.import_module(tab)
    view = module.get_html(client)

    view_dict = dict(
        view=view,
        tab_name=tab.rpartition(".")[2],
    )

    tab_name, tab_order = parse_view_docstring(module)
    if tab_name:
        view_dict['tab_name'] = tab_name
    if tab_order:
        view_dict['tab_order'] = tab_order
    return view_dict


def parse_view_docstring(module):
    """
    Parse the docstring of the view module. This docstring can contain the tab name and tab order.
    To add this information to a docstring use the following syntax

    ::

        tab_name: Financieel
        tab_order: 2


    Args:
        module (ModuleType): An imported view.

    Returns:
        tuple: tuple containing:
            str: The name of the tab\n
            int: A value to order the tabs

    """
    tab_name = tab_order = None
    doc = module.__doc__
    if doc:
        for line in doc.split("\n"):
            if line.startswith("tab_name:"):
                tab_name = line.lstrip("tab_name:").strip()
            if line.startswith("tab_order:"):
                tab_order = int(line.lstrip("tab_order:").strip())
            if tab_name and tab_order:
                break
    return tab_name, tab_order


def client_page_body(client, project):
    """
    Build the client body.

    Args:
        client (str): The client
        project (str): The project to immediately load

    Returns:
        dash.development.base_component.Component: A dash component of the page.

    """
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
    """
    Get the page body. When there is a client return the client, else the main page. In case of errors return the error
    page.

    Args:
        client (str): The client
        project (str): The project to immediately load

    Returns:
        dash.development.base_component.Component: A dash component of the page.

    """
    if client:
        try:
            body = client_page_body(client, project)
        except (ImportError, FileNotFoundError):
            body = error.get_body()
    else:
        body = main_page.get_body()

    return html.Div(
        children=body,
        id="mainContainer",
        style={"display": "flex", "flex-direction": "column"},
    )


def get_layout(pathname="/", brand=""):
    """
    Returns the layout dependent on current url.

    Args:
        pathname (str): The url path after the domain. For example :code:`/tmobile`
        brand (str): The text to be on the left in the menu bar.

    Returns:
        dash.development.base_component.Component: A dash component of the page.

    """
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
