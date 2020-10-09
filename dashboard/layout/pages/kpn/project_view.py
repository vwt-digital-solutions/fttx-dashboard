import dash_html_components as html

import dash_core_components as dcc
import config
colors = config.colors_vwt


def get_html(client):
    return [
        html.Div(
            id=f'indicators-{client}',
            className="container-display",
            hidden=True,
        ),
        html.Div(
            id=f'indicators-{client}',
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
        )
    ]
