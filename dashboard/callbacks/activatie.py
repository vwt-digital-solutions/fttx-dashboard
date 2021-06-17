from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

import config
from app import app
from data.data import (fetch_data_for_barchart_ActualConnectionTypes,
                       fetch_data_for_barchart_voortgang_activatie,
                       fetch_data_for_project_boxes_activatie,
                       fetch_data_for_timeseries_voortgang_activatie)
from layout.components.graphs.horizontal_bar_chart import \
    get_fig_ActualConnectionTypes
from layout.components.graphs.no_graph import no_graph
from layout.components.graphs.project_activatie_afsluit_planned import \
    get_fig as get_fig_activatie
from layout.components.graphs.project_activatie_afsluit_planned_dif import \
    get_fig as get_fig_activatie_dif
from layout.components.list_of_boxes import global_info_list

for client in config.client_config.keys():  # noqa: C901

    @app.callback(
        Output(f"graph-actual-connection-type-activatie-{client}", "figure"),
        [
            Input(f"project-dropdown-{client}", "value"),
            Input(
                f"date-picker-actual-connection-type-activatie-{client}", "start_date"
            ),
            Input(f"date-picker-actual-connection-type-activatie-{client}", "end_date"),
        ],
    )
    def actual_connection_type(project, start_date, end_date, client=client):
        if project:
            ordered_dict = fetch_data_for_barchart_ActualConnectionTypes(
                project, client, start_date, end_date
            )
            if ordered_dict:
                fig = get_fig_ActualConnectionTypes(ordered_dict)
            else:
                fig = no_graph(title="ActualConnection types", text="No Data")
            return fig

        raise PreventUpdate

    @app.callback(
        Output(f"realised-connections-activatie-{client}", "figure"),
        [Input(f"project-dropdown-{client}", "value")],
    )
    def realised_connections(project, client=client):
        if project:
            fig = get_fig_activatie(
                data=fetch_data_for_timeseries_voortgang_activatie(project, client)
            )
            return fig
        raise PreventUpdate

    @app.callback(
        Output(f"realised-connections-activatie-dif-{client}", "figure"),
        [Input(f"project-dropdown-{client}", "value")],
    )
    def realised_connections_dif(project, client=client):
        if project:
            fig = get_fig_activatie_dif(
                data=fetch_data_for_barchart_voortgang_activatie(project, client)
            )
            return fig
        raise PreventUpdate

    @app.callback(
        Output(f"activatie-indicators-{client}", "children"),
        [Input(f"project-dropdown-{client}", "value")],
    )
    def update_activatie_indicators(dropdown_selection, client=client):
        if dropdown_selection is None:
            raise PreventUpdate

        activatie_indicator_info = global_info_list(
            fetch_data_for_project_boxes_activatie(
                project=dropdown_selection, client=client
            )
        )

        return activatie_indicator_info
