from dash import callback_context
import dash_html_components as html
from dash.exceptions import PreventUpdate

import config

from dash.dependencies import Input, Output, State

from app import app
from layout.components.indicator import indicator

for client in config.client_config.keys():
    @app.callback(
        Output(f'capacity-phase-Schouwen-{client}', 'n_clicks'),
        [
            Input("project-tabs", "active_tab")
        ]
    )
    def switch_tab(active_tab):
        if active_tab == "tab-capaciteit":
            return 1
        return [None]

    @app.callback(
        Output(f"capacity-indicators-{client}", "children"),
        [
            Input(f'capacity-phase-{phase}-{client}', 'n_clicks') for phase in
            ['Schouwen', 'Lassen', 'Graven', 'HASsen']
        ]
    )
    def phase_buttons(schouwen, lassen, graven, hassen):
        if not callback_context.triggered:
            raise PreventUpdate
        trigger = callback_context.triggered[0]['prop_id'].split("-")[2]
        return [
            html.Div(
                className="container-display",
                children=html.H2(f"Capaciteit voor {trigger}"),
            ),
            html.Div(
                className="container-display",
                children=[
                    indicator(value=500,
                              previous_value=480,
                              title="Wat ga ik doen?"),
                    indicator(value=500,
                              previous_value=480,
                              title="Wat heb ik afgesproken?"),
                    indicator(value=500,
                              previous_value=480,
                              title="Wat kan ik doen?"),
                    indicator(value=500,
                              previous_value=480,
                              title="Wat moet ik doen?")

                ]
            )
        ]

    @app.callback(
        Output(f"more-info-collapse-{client}", "is_open"),
        [Input(f"collapse-button-{client}", "n_clicks")],
        [State(f"more-info-collapse-{client}", "is_open")],
    )
    def toggle_collapse(n, is_open):
        if n:
            return not is_open
        return is_open
