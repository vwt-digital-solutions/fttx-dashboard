import dash_html_components as html
import dash_bootstrap_components as dbc


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
            html.Div(
                id=f"capacity-test-{client}",
                className="container-display",

            )
        ]
    )
