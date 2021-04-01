import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html

import config
from app import toggles
from data import collection
from data.data import fetch_data_for_performance_graph, no_graph
from data.graph import ftu_table
from layout.components.figure import figure
from layout.components.graphs import performance_chart

colors = config.colors_vwt


def get_html(client):
    return [
        html.Div(
            children=html.Div(
                children=[
                    dcc.Dropdown(
                        id=f"year-dropdown-{client}",
                        placeholder="Select a year",
                        clearable=False,
                    ),
                ],
                className="column",
                style={"width": "77px"},
            ),
            className="container-display",
        ),
        html.Div(
            children=[],
            id=f"info-container-year-{client}",
        ),
        html.Div(
            className="container-display",
            children=[
                figure(
                    graph_id=f"month-overview-year-{client}",
                    figure=no_graph(title="Jaaroverzicht", text="Loading..."),
                ),
                figure(
                    graph_id=f"week-overview-year-{client}",
                    figure=no_graph(title="Maandoverzicht", text="Loading..."),
                ),
                figure(
                    container_id=f"pie_chart_overview_{client}_container",
                    graph_id=f"pie_chart_overview-year_{client}",
                    figure=no_graph(title="Opgegeven reden na", text="Loading..."),
                ),
            ],
        ),
        html.Div(children=get_performance_graph(client), className="container-display"),
        html.Div(children=get_ftu_table(client), className="container-display"),
    ]


def get_search_bar(client, project):
    return [
        html.Div(
            [
                dcc.Dropdown(
                    id=f"project-dropdown-{client}",
                    options=collection.get_document(
                        collection="Data", client=client, graph_name="project_names"
                    ).get("filters"),
                    value=project if project else None,
                    placeholder="Select a project",
                )
            ],
            className="two-third column",
        ),
        html.Div(
            [
                dbc.Button(
                    "Reset overzicht",
                    id=f"overview-reset-{client}",
                    n_clicks=0,
                    style={
                        "margin-left": "10px",
                        "margin-right": "55px",
                        "background-color": colors["vwt_blue"],
                    },
                )
            ]
        ),
        html.Div(
            [
                dbc.Button(
                    "Terug naar overzicht alle projecten",
                    id="overzicht-button-" + client,
                    style={"background-color": colors["vwt_blue"]},
                )
            ],
            className="one-third column",
        ),
    ]


def get_performance_graph(client):
    if toggles.transform_frontend_newindicator:
        graph = figure(
            container_id="graph_speed_c",
            graph_id=f"project-performance-{client}",
            figure=performance_chart.get_fig(
                fetch_data_for_performance_graph(year="2021", client=client)
            ),
        )
    else:
        graph = figure(
            container_id="graph_speed_c",
            graph_id=f"project-performance-{client}",
            figure=collection.get_graph(
                client=client, graph_name="project_performance"
            ),
        )
    return graph


def get_ftu_table(client):
    print(f"CLIENT: {client}")
    ftu_data = collection.get_document(
        collection="ProjectInfo", graph_name="project_dates", client=client
    )
    table = ftu_table(ftu_data, client)
    return html.Div(
        table,
        id=f"FTU_table_c_{client}",
        className="pretty_container column",
        hidden=False,
    )
