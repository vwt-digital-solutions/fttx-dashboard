from dash import callback_context
from dash.exceptions import PreventUpdate

import config

from dash.dependencies import Input, Output, State

from app import app
from layout.components.capacity.capacity_summary import capacity_summary

for client in config.client_config.keys():
    @app.callback(
        Output(f'capacity-phase-Schouwen-{client}', 'n_clicks'),
        [
            Input("project-tabs", "active_tab")
        ]
    )
    def switch_tab(active_tab):
        """
        When going to the capacity tab the first tab should be activated. This is done by setting n_clicks for the
        target tab, causing :func:`phase_buttons` to be triggered.

        Args:
            active_tab: The id of the tab that was clicked.

        Returns:
            int: Sets the n_clicks for the target tab to 1.
        """
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
        """
        This callback switches the view based on which tab is clicked. The input variables are not used as
        callback_context.triggered is used to access which trigger caused the callback.

        When the view changes the data is retrieved and rendered in the dashboard.

        Args:
            schouwen:
            lassen:
            graven:
            hassen:

        Returns:
            A rendered view with the relevant data for the selected phase.
        """
        if not callback_context.triggered:
            raise PreventUpdate
        phase = callback_context.triggered[0]['prop_id'].split("-")[2]
        return capacity_summary(phase)

    @app.callback(
        Output(f"more-info-collapse-{client}", "is_open"),
        [Input(f"collapse-button-{client}", "n_clicks")],
        [State(f"more-info-collapse-{client}", "is_open")],
    )
    def toggle_collapse(n, is_open):
        """
        This callback opens and collapses the more info panel.

        Args:
            n:
            is_open:

        Returns:

        """
        if n:
            return not is_open
        return is_open
