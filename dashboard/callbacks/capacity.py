from dash import callback_context
from dash.exceptions import PreventUpdate

import config

from dash.dependencies import Input, Output, State

from app import app
from data.collection import get_document
from data.data import no_graph
from layout.components.capacity.capacity_summary import capacity_summary

import plotly.express as px

import pandas as pd

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
            Output(f"more-info-graph-{client}", "figure"),
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

        freq = 'week'  # hier moet zometeen de keuze week of month gemaakt worden via filter oid

        indicator_values = dict(target=0, werkvoorraad=0, capacity_ideal=0, poc_ideal=0)
        timeseries = dict(target=pd.Series(), werkvoorraad=pd.Series(), capacity_ideal=pd.Series(), poc_ideal=pd.Series())
        for key in indicator_values:
            indicator_dict = get_document("Lines", line=key + '_indicator', **selection_settings)
            if indicator_dict:
                indicator_values[key] = int(indicator_dict['this_' + freq])
                timeseries[key] = pd.Series(indicator_dict['series_' + freq])

        phase_name = config.capacity_phases[phase].get('name')

        if all([True for k, v in timeseries.items() if v is not None]):
            line_graph = px.line(timeseries)
            line_graph.update_layout(
                height=500,
                paper_bgcolor=config.colors_vwt['paper_bgcolor'],
                plot_bgcolor=config.colors_vwt['plot_bgcolor'],
            )
        else:
            line_graph = no_graph("No data")
        return [capacity_summary(phase_name=phase_name,
                                 target=indicator_values['target'],
                                 werkvoorraad=indicator_values['werkvoorraad'],
                                 capacity=indicator_values['capacity_ideal'],
                                 poc=indicator_values['poc_ideal']),
                line_graph
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
