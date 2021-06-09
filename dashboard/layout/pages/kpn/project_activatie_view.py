"""
Activatie view
==========
tab_name: Activatie
tab_order: 2
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
            children=[],
            id=f"activatie-indicators-{client}",
        ),
        html.Div(
            [
                figure(
                    figure=no_graph(
                        title="Gerealiseerde aansluitingen", text="Loading..."
                    ),
                    container_id=f"realised-connections-activatie-{client}-container",
                    graph_id=f"realised-connections-activatie-{client}",
                ),
                figure(
                    figure=no_graph(title="ActualConnection types", text="Loading..."),
                    graph_id=f"graph-actual-connection-type-activatie-{client}",
                ),
                # figure(
                #     figure=no_graph(
                #         title="Prognose activatie", text="No Data"
                #     ),
                #     container_id=f"prognose-activatie-{client}-container",
                #     graph_id=f"prognose-activatie-{client}",
                # )
            ],
            id="main_graphs",
            className="container-display",
        ),
    ]
