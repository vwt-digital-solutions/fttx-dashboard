import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html

import config
from layout.components.figure import figure
from layout.components.graphs.no_graph import no_graph


def capacity_template(client):
    """
    The template for the capacity view.

    The phase selector is shown at the top, followed by the summarized data that answers the most important questions.
    Lastly there is a button that lets you open the more information panel.

    Args:
        client:

    Returns:
        html.Div: The capacity view.
    """
    return html.Div(
        id=f"cookie-factory-{client}",
        children=[
            dcc.Store(id=f"memory_phase_{client}", data="geulen"),
            dbc.Row(
                id=f"selection-menu-{client}",
                children=[
                    html.Div(
                        className=f"container-display-{client}",
                        id=f"capacity-phase-{client}",
                        children=dbc.ButtonGroup(
                            [
                                dbc.Button(
                                    phase_data.get("name"),
                                    id=f"capacity-phase-{phase}-{client}",
                                )
                                for phase, phase_data in config.capacity_phases.items()
                            ]
                        ),
                    ),
                    html.Div(
                        className="container-display",
                        children=dcc.Dropdown(
                            id=f"frequency-selector-{client}",
                            options=[
                                {"label": "Week", "value": "week"},
                                {"label": "Maand", "value": "month"},
                            ],
                            value="week",
                            clearable=False,
                            style={
                                "color": config.colors_vwt.get("darkgray"),
                                "margin-left": "10px",
                                "width": "150%",
                            },
                        ),
                    ),
                ],
            ),
            html.Div(
                id=f"capacity-indicators-{client}",
            ),
            dbc.Row(
                id=f"capacity-info-{client}",
                children=dbc.Col(
                    [
                        dbc.Button(
                            "Meer informatie",
                            id=f"collapse-button-{client}",
                            className="mb-3",
                            style={"background-color": config.colors_vwt["vwt_blue"]},
                        ),
                        dbc.Collapse(
                            figure(
                                graph_id=f"more-info-graph-{client}",
                                figure=no_graph(
                                    title="Verdieping Capaciteit", text="Geen data..."
                                ),
                            ),
                            id=f"more-info-collapse-{client}",
                        ),
                    ],
                    width=12,
                ),
            ),
        ],
    )
