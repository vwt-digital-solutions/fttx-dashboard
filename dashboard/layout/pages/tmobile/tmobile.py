import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc

from layout.components.header import header
from layout.pages.tmobile import overview, project_view
from data import collection
import config

colors = config.colors_vwt


# APP LAYOUT
def get_body():
    page = html.Div(
        [
            header("Status projecten T-Mobile in 2020"),

            html.Div(
                id="tmobile-overview",
                children=overview.get_html("t-mobile"),
            ),
            html.Div(
                [
                    html.Div(
                        [dcc.Dropdown(id='project-dropdown-tmobile',
                                      options=collection.get_document(collection="Data", client="t-mobile",
                                                                      graph_name="project_names")['filters'],
                                      value=None)],
                        className="two-third column",
                    ),
                    html.Div(
                        [dbc.Button('Terug naar overzicht alle projecten',
                                    id='overzicht-button-tmobile',
                                    style={'background-color': colors['vwt_blue']})],
                        className="one-third column",
                    ),
                ],
                className="container-display",
                id="title",
            ),
            html.Div(
                className="container-display",
                children=[html.Button('Reset', id='overview-reset', n_clicks=0, style={"margin-left": "10px"})],
            ),
            html.Div(
                style={'display': 'none'},
                id="tmobile-project-view",
                children=project_view.get_html(""),
            ),
        ],
        id="mainContainer",
        style={"display": "flex", "flex-direction": "column"},
    )
    return page
