import dash_html_components as html
import dash_bootstrap_components as dbc

from data.data import no_graph
from layout.components.figure import figure


def capacity_template(client):
    return html.Div(
        id=f'cookie-factory-{client}',
        children=[
            html.Div(
                className="container-display",
                id=f"capacity-phase-{client}",
                children=dbc.ButtonGroup(
                    [
                        dbc.Button(phase, id=f"capacity-phase-{phase}-{client}")
                        for i, phase in enumerate(['Schouwen', 'Lassen', 'Graven', 'HASsen'])
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
