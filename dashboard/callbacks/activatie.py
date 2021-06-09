import collections

from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

import config
from app import app
from data import collection
from data.data import fetch_data_for_project_boxes_activatie
from layout.components.graphs.horizontal_bar_chart import get_fig
from layout.components.graphs.project_activatie_afsluit_planned import \
    get_fig as get_fig_activatie
from layout.components.list_of_boxes import global_info_list

colors = config.colors_vwt

for client in config.client_config.keys():  # noqa: C901

    @app.callback(
        Output(f"graph-actual-connection-type-activatie-{client}", "figure"),
        [Input(f"project-dropdown-{client}", "value")],
    )
    def actual_connection_type(project, client=client):
        if project:
            data = collection.get_document(
                collection="Indicators",
                project=project,
                client=client,
                graph_name="ActualConnectionTypeIndicator",
            )

            new_dict = dict()
            for key, value in data.items():
                new_dict[int(float(key))] = value

            ordered_dict = collections.OrderedDict(sorted(new_dict.items()))

            if data:
                bar = {
                    "name": "Actual Connections",
                    "x": list(ordered_dict.values()),
                    "y": list(ordered_dict.keys()),
                    "color": colors.get("vwt_blue"),
                }

                fig = get_fig(bar)
                fig.update_layout(
                    yaxis=dict(type="category", title="Type aansluiting"),
                    xaxis=dict(title="Aantal"),
                )

                return fig
        raise PreventUpdate

    @app.callback(
        Output(f"realised-connections-activatie-{client}", "figure"),
        [Input(f"project-dropdown-{client}", "value")],
    )
    def realised_connections(project, client=client):
        if project:
            data = dict()
            data["afsluit_indicator"] = collection.get_week_series_from_document(
                collection="Indicators",
                project=project,
                client=client,
                line="AfsluitIntegratedIndicator",
            )

            data["planned_indicator"] = collection.get_week_series_from_document(
                collection="Indicators",
                project=project,
                client=client,
                line="PlannedActivationIntegratedIndicator",
            )

            fig = get_fig_activatie(data=data)
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
