import dash_core_components as dcc
import dash_html_components as html

from layout.components.header import header
from layout.pages.tmobile import overview
import config

colors = config.colors_vwt


# APP LAYOUT
def get_body():
    client = 'kpn'
    page = html.Div(
        [
            dcc.Store(id="aggregate_data",
                      data=None),
            dcc.Store(id="aggregate_data2",
                      data=None),
            header("Status projecten KPN in 2020"),
            html.Div(
                id=f"{client}-overview",
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
            html.Div(
                id='indicators-kpn',
                className="container-display",
                hidden=True,
            ),
            html.Div(
                id='indicators-kpn',
                className="container-display",
                hidden=True,
            ),
            html.Div(
                [
                    html.Div(
                        [dcc.Graph(id="graph_prog")],
                        id='graph_prog_c',
                        className="pretty_container column",
                        hidden=True,
                    ),
                    html.Div(
                        [dcc.Graph(id="Bar_LB")],
                        id='Bar_LB_c',
                        className="pretty_container column",
                        hidden=True,
                    ),
                    html.Div(
                        [dcc.Graph(id="Bar_HB")],
                        id='Bar_HB_c',
                        className="pretty_container column",
                        hidden=True,
                    ),
                    html.Div(
                        [dcc.Graph(id="Pie_NA_c")],
                        id='Pie_NA_cid',
                        className="pretty_container column",
                        hidden=True,
                    ),
                ],
                id="main_graphs",
                className="container-display",
            ),
            html.Div(
                [
                    html.Div(
                        [dcc.Graph(id="geo_plot")],
                        id='geo_plot_c',
                        className="pretty_container column",
                        hidden=True,
                    ),
                ],
                id="details",
                className="container-display",
            ),
            html.Div(
                [
                    html.Div(
                        id='table_c',
                        className="pretty_container",
                        hidden=True,
                    ),
                ],
                id="details",
                className="container-display",
            ),
        ],
        id="mainContainer",
        style={"display": "flex", "flex-direction": "column"},
    )
    return page
