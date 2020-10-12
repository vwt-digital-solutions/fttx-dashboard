import dash_html_components as html

import dash_core_components as dcc
import config
colors = config.colors_vwt


def get_html(client):
    return [
        html.Div(
            id=f'indicators-{client}',
            className="container-display",
        ),
        html.Div(
            id=f'indicators-{client}',
            className="container-display",
        ),
        html.Div(
            [
                html.Div(
                    [dcc.Graph(id=f"graph_prog-{client}")],
                    id='graph_prog_c',
                    className="pretty_container column",
                ),
                html.Div(
                    [dcc.Graph(id="Bar_LB")],
                    id='Bar_LB_c',
                    className="pretty_container column",
                ),
                html.Div(
                    [dcc.Graph(id="Bar_HB")],
                    id='Bar_HB_c',
                    className="pretty_container column",
                ),
                html.Div(
                    [dcc.Graph(id="Pie_NA_c")],
                    id='Pie_NA_cid',
                    className="pretty_container column",
                ),
            ],
            id="main_graphs",
            className="container-display",
        )
    ]
