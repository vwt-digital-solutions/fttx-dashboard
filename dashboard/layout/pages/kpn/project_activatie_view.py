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
from datetime import date

colors = config.colors_vwt


def get_html(client):
    return [
        dcc.Store(id="aggregate_data", data=None),
        dcc.Store(id="aggregate_data2", data=None),
        dcc.Store(id=f"status-count-filter-{client}", data={}),
        html.Div(
            children=html.Div(
                id=f"activatie-indicators-{client}", className="container-display"
            )
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
                html.Div(
                    [
                        figure(
                            figure=no_graph(title="ActualConnection types", text="Loading..."),
                            graph_id=f"graph-actual-connection-type-activatie-{client}",
                            className=None
                        ),
                        dcc.DatePickerRange(
                            id=f'date-picker-actual-connection-type-actiatie-{client}',
                            initial_visible_month=date(date.today().year, date.today().month, date.today().day),
                            clearable=True,
                        )
                    ],
                    className='pretty_container column',
                )

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
