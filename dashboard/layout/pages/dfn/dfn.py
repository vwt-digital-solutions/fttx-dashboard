import dash_core_components as dcc
import dash_html_components as html

from layout.components.header import header
from layout.components import overview
import config
from layout.pages.dfn import project_view

colors = config.colors_vwt


# APP LAYOUT
def get_body():
    client = 'dfn'
    page = html.Div(
        [
            dcc.Store(id="aggregate_data",
                      data=None),
            dcc.Store(id="aggregate_data2",
                      data=None),
            header("Status projecten DFN in 2020"),
            html.Div(
                overview.get_search_bar(client),
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
            ),
        ],
        id="mainContainer",
        style={"display": "flex", "flex-direction": "column"},
    )
    return page
