import dash_html_components as html

from layout.components.header import header
from layout.pages.tmobile import overview, project_view
import config
import importlib

colors = config.colors_vwt


# APP LAYOUT
def get_body(client):
    importlib.import_module(f"layout.pages.{client}.project_view")
    page = html.Div(
        [
            header("Status projecten T-Mobile in 2020"),

            html.Div(
                id=client + "-overview",
                children=overview.get_html(client),
            ),
            html.Div(
                overview.get_search_bar(client),
                className="container-display",
                id="title",
            ),
            html.Div(
                overview.get_performance(client),
                className="container-display",
            ),
            # Projectspecifieke view
            html.Div(
                style={'display': 'none'},
                id=client + "-project-view",
                children=project_view.get_html(client),
            ),
        ],
        id="mainContainer",
        style={"display": "flex", "flex-direction": "column"},
    )
    return page
