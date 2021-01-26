import dash_html_components as html
import dash_bootstrap_components as dbc

import config
from data.data import no_graph
from layout.components.figure import figure


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
        id=f'cookie-factory-{client}',
        children=[
            html.Div(
                className="container-display",
                id=f"capacity-phase-{client}",
                children=dbc.ButtonGroup(
                    [
                        dbc.Button(phase_data.get("name"), id=f"capacity-phase-{phase}-{client}")
                        for phase, phase_data in config.capacity_phases.items()
                    ]
                )
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
                            color="primary",
                        ),
                        dbc.Collapse(
                            figure(
                                graph_id=f"more-info-graph-{client}",
                                figure=no_graph(title="Verdieping Capaciteit", text='Geen data...')
                            ),
                            id=f"more-info-collapse-{client}",
                        ),
                    ],
                    width=12
                )

            )
        ]
    )