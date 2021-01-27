from dash import callback_context
from dash.exceptions import PreventUpdate

import config

from dash.dependencies import Input, Output, State

from app import app
from data.collection import get_document
from layout.components.capacity.capacity_summary import capacity_summary

# import plotly.express as px

# import pandas as pd

for client in config.client_config.keys():
    @app.callback(
        Output(f'capacity-phase-geulen-{client}', 'n_clicks'),
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
        [
            Output(f"capacity-indicators-{client}", "children"),
            # Output(f"more-info-graph-{client}", "figure"),
        ],
        [
            Input(f'capacity-phase-{phase}-{client}', 'n_clicks') for phase in
            config.capacity_phases.keys()

        ],
        [
            State(f'project-dropdown-{client}', 'value')
        ]
    )
    def phase_buttons(*args, client=client):
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

        project = callback_context.states[f'project-dropdown-{client}.value']

        selection_settings = dict(
            client=client, project=project, phase=phase
        )

        target_number = get_document("Lines", line='target_indicator', **selection_settings)
        if target_number:
            target_number = target_number['this_week']
        else:
            target_number = 0

        # werkvoorraad = pd.Series(get_document("Lines", line='werkvoorraad_indicator', **selection_settings))
        # werkvoorraad = werkvoorraad[werkvoorraad != 0].sort_index()

        # capacity = pd.Series(get_document("Lines", line='capacity_ideal_indicator', **selection_settings))
        # capacity = capacity[capacity != 0].sort_index()

        # poc = pd.Series(get_document("Lines", line='poc_ideal_indicator', **selection_settings))
        # poc = poc[poc != 0].sort_index()

        phase_name = config.capacity_phases[phase].get('name')

        # target_number = round(target.tail(1).item()) if not target.empty else 0
        # werkvoorraad_number = round(werkvoorraad.tail(1).item()) if not werkvoorraad.empty else 0
        # capacity_number = round(capacity.tail(1).item()) if not capacity.empty else 0
        # poc_number = round(poc.tail(1).item()) if not poc.empty else 0

        # df = pd.DataFrame([pd.Series(), pd.Series(), pd.Series(), pd.Series()])
        # df.columns = ['target', 'werkvoorraad', 'capacity', 'poc']

        return [capacity_summary(phase_name=phase_name,
                                 target=target_number,
                                 werkvoorraad=target_number,
                                 capacity=target_number,
                                 poc=target_number),
                # px.line(df, height=400)
                ]

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
