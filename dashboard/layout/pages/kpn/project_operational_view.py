"""
Operational view
==========
tab_name: Operationeel
tab_order: 1
"""


import dash_core_components as dcc
import dash_html_components as html

import config
from layout.components.figure import figure
from layout.components.graphs.no_graph import no_graph

colors = config.colors_vwt


def get_html(client):
    return [
        dcc.Store(id="aggregate_data", data=None),
        dcc.Store(id="aggregate_data2", data=None),
        dcc.Store(id=f"status-count-filter-{client}", data={}),
        html.Div(
            id=f"indicators-{client}",
            className="container-display",
        ),
        html.Div(
            [
                figure(
                    figure=no_graph(
                        title="Progress of HPend over time", text="Loading..."
                    ),
                    graph_id=f"graph_prog-{client}",
                ),
                figure(
                    figure=no_graph(
                        title="Status oplevering per fase (LB)", text="Loading..."
                    ),
                    container_id=f"status-counts-laagbouw-{client}-container",
                    graph_id=f"status-counts-laagbouw-{client}",
                ),
                figure(
                    figure=no_graph(
                        title="Status oplevering per fase (HB)", text="Loading..."
                    ),
                    container_id=f"status-counts-hoogbouw-{client}-container",
                    graph_id=f"status-counts-hoogbouw-{client}",
                ),
                figure(
                    figure=no_graph(title="Opgegeven reden na", text="Loading..."),
                    container_id=f"redenna_project_{client}_container",
                    graph_id=f"redenna_project_{client}",
                ),
            ],
            id="main_graphs",
            className="container-display",
        ),
    ]
