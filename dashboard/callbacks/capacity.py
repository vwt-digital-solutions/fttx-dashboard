from dash import callback_context
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

import config
from app import app
from data.data import fetch_data_productionstatus
from layout.components.capacity.capacity_summary import capacity_summary
from layout.components.graphs import productionstatus
from layout.components.graphs.no_graph import no_graph

colors = config.colors_vwt

for client in config.client_config.keys():  # noqa: C901

    @app.callback(
        Output(f"capacity-phase-geulen-{client}", "n_clicks"),
        [Input("project-tabs", "active_tab")],
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
            Output(f"memory_phase_{client}", "data"),
        ],
        [
            Input(f"capacity-phase-{phase}-{client}", "n_clicks")
            for phase in config.capacity_phases.keys()
        ]
        + [
            Input(f"frequency-selector-{client}", "value"),
            Input(f"project-dropdown-{client}", "value"),
        ],
        [State(f"memory_phase_{client}", "data")],
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

        phase = callback_context.triggered[0]["prop_id"].split("-")[2]

        if config.capacity_phases.get(phase) is None:
            phase = callback_context.states[f"memory_phase_{client}.data"]
            phase_name = config.capacity_phases[phase].get("name")
        else:
            phase_name = config.capacity_phases[phase].get("name")

        project = callback_context.inputs[f"project-dropdown-{client}.value"]

        freq = callback_context.inputs[f"frequency-selector-{client}.value"]

        indicator_values, timeseries, line_graph_bool = fetch_data_productionstatus(
            project, client, freq, phase_name
        )

        if line_graph_bool:
            line_graph = productionstatus.get_fig(timeseries)
        else:
            line_graph = no_graph("No data")

        return [
            capacity_summary(
                phase_name=phase_name,
                target=indicator_values["Target"],
                work_stock=indicator_values["Hoeveelheid Werkvoorraad"],
                capacity=indicator_values["Verwacht verloop"],
                poc=indicator_values["Ideaal verloop"],
                unit=config.capacity_phases[phase].get("unit"),
            ),
            line_graph,
            phase,
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
