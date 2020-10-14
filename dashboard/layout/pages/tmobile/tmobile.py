import dash_html_components as html

from layout.components.header import header
from layout.pages.tmobile import project_view
from layout.components import overview
import config
# import importlib

colors = config.colors_vwt

client = "tmobile"


# APP LAYOUT
def get_body(client, project=""):
    # importlib.import_module(f"layout.pages.{client}.project_view")
    page = html.Div(
        [
            header("Status projecten T-Mobile in 2020"),

            html.Div(
                overview.get_search_bar(client, project),
                className="container-display",
                id="title",
            ),
            html.Div(
                id=f"{client}-overview",
                children=overview.get_html(client),
            ),
            # Projectspecifieke view
            html.Div(
                style={'display': 'none'},
                id=f"{client}-project-view",
                children=project_view.get_html(client),
            ),
        ],
        id="mainContainer",
        style={"display": "flex", "flex-direction": "column"},
    )
    return page
