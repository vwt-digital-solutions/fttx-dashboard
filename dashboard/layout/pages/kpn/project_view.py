import dash_html_components as html

import dash_core_components as dcc
import config
from data.data import no_graph
from layout.components.figure import figure

colors = config.colors_vwt


def get_html(client):
    return [
        dcc.Store(id="aggregate_data",
                  data=None),
        dcc.Store(id="aggregate_data2",
                  data=None),
        dcc.Store(id=f"status-count-filter-{client}"),
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
                figure(figure=no_graph(title="Status oplevering per fase (LB)", text='Loading...'),
                       container_id=f"status-counts-laagbouw-{client}-container",
                       graph_id=f"status-counts-laagbouw-{client}"),
                figure(figure=no_graph(title="Status oplevering per fase (HB)", text='Loading...'),
                       container_id=f"status-counts-hoogbouw-{client}-container",
                       graph_id=f"status-counts-hoogbouw-{client}"),
                figure(container_id=f"redenna_project_{client}_container",
                       graph_id=f"redenna_project_{client}",
                       figure=no_graph(title="Opgegeven reden na", text='Loading...'))
            ],
            id="main_graphs",
            className="container-display",
        )
    ]
