from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

import config
from app import app
from data import collection
from data.data import fetch_data_for_project_boxes_activatie
from layout.components.graphs.horizontal_bar_chart import get_fig
from layout.components.graphs.no_graph import no_graph
from layout.components.graphs.project_activatie_afsluit_planned import \
    get_fig as get_fig_activatie
from layout.components.graphs.project_activatie_afsluit_planned_dif import \
    get_fig as get_fig_activatie_dif
from layout.components.list_of_boxes import global_info_list

colors = config.colors_vwt

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
            data = collection.get_documents(
                collection="Indicators",
                project=project,
                client=client,
                line="ConnectionTypeIndicator",
            )

            if data:
                unpacked_data = {}
                for line in data:
                    phase = line.get("phase")
                    series = line.get("record").get("series_week")

                    if start_date and end_date:
                        category_size = sum(
                            [
                                v
                                for k, v in series.items()
                                if ((k >= start_date) & (k <= end_date))
                            ]
                        )
                    else:
                        category_size = sum(list(series.values()))

                    unpacked_data[phase] = category_size

                ordered_dict = {
                    int(float(k)): v
                    for k, v in sorted(unpacked_data.items(), key=lambda item: item[1])
                }
                bar = {
                    "name": "Actual Connections",
                    "x": list(ordered_dict.values()),
                    "y": list(ordered_dict.keys()),
                    "color": colors.get("vwt_blue"),
                    "text": "x",
                    "title": "Categorisatie van gerealiseerde aansluitingen in BP",
                }

                fig = get_fig(bar)
                fig.update_layout(
                    yaxis=dict(type="category", title="Type aansluiting"),
                    xaxis=dict(title="Aantal"),
                )

                return fig
            else:
                return no_graph(title="ActualConnection types", text="No Data")

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
        Output(f"realised-connections-activatie-dif-{client}", "figure"),
        [Input(f"project-dropdown-{client}", "value")],
    )
    def realised_connections_dif(project, client=client):
        if project:
            data = dict()
            data["afsluit_indicator"] = collection.get_month_series_from_document(
                collection="Indicators",
                project=project,
                client=client,
                line="AfsluitIndicator",
            )

            data["planned_indicator"] = collection.get_month_series_from_document(
                collection="Indicators",
                project=project,
                client=client,
                line="PlannedActivationIndicator",
            )
            fig = get_fig_activatie_dif(data=data)
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
