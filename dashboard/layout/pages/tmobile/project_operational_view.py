"""
Operational view
==========
tab_name: Operationeel
tab_order: 1
"""

import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html

import config
from layout.components.figure import figure
from layout.components.graphs.no_graph import no_graph

colors = config.colors_vwt


def get_html(client):
    return [
        dcc.Store(id=f"status-count-filter-{client}"),
        dcc.Store(id=f"indicator-data-{client}"),
        html.Div(id=f"indicators-{client}"),
        html.Div(
            className="container-display",
            children=[
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
                html.Div(
                    className="pretty_container column",
                    children=[
                        figure(
                            container_id=f"redenna_project_{client}_container",
                            graph_id=f"redenna_project_{client}",
                            figure=no_graph(
                                title="Opgegeven reden na", text="Loading..."
                            ),
                            className="",
                        ),
                        html.A(
                            dbc.Button("Download", className="ml-auto"),
                            id=f"project-redenna-download-{client}",
                            href="",
                        ),
                    ],
                ),
            ],
        ),
    ]
