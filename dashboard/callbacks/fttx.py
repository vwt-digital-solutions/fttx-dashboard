from datetime import datetime

import dash
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

import config
from app import app
from data import collection
from data.data import (fetch_data_for_month_overview,
                       fetch_data_for_overview_boxes,
                       fetch_data_for_performance_graph,
                       fetch_data_for_project_info_table,
                       fetch_data_for_redenna_overview,
                       fetch_data_for_status_barchart,
                       fetch_data_for_status_redenna_piechart,
                       fetch_data_for_week_overview)
from layout.components.graphs import (completed_status_counts_bar,
                                      overview_bar_chart, performance_chart,
                                      project_info_table,
                                      redenna_overview_chart,
                                      redenna_status_pie)
from layout.components.graphs.no_graph import no_graph
from layout.components.list_of_boxes import global_info_list

for client in config.client_config.keys():  # noqa: C901

    @app.callback(
        [Output(f"{client}-overview", "style")],
        [Input(f"project-dropdown-{client}", "value")],
    )
    def overview(dropdown_selection):
        if dropdown_selection:
            return [{"display": "none"}]
        return [{"display": "block"}]

    @app.callback(
        [
            Output(f"{client}-project-view", "style"),
        ],
        [Input(f"project-dropdown-{client}", "value")],
    )
    def project_view(dropdown_selection):
        if dropdown_selection:
            return [{"display": "block"}]
        return [{"display": "none"}]

    @app.callback(
        [
            Output(f"project-dropdown-{client}", "value"),
        ],
        [
            Input(f"project-performance-year-{client}", "clickData"),
            Input(f"overzicht-button-{client}", "n_clicks"),
        ],
    )
    def update_dropdown(project_performance_click, overzicht_click):
        ctx = dash.callback_context
        for trigger in ctx.triggered:
            if trigger["prop_id"] == list(ctx.inputs.keys())[0]:
                return [project_performance_click["points"][0]["text"]]
            elif trigger["prop_id"] == list(ctx.inputs.keys())[1]:
                return [None]
        return [None]

    @app.callback(
        [Output(f"project-dropdown-{client}", "options")],
        [Input(f"{client}-overview", "style")],
    )
    def load_dropdown(dummy_data, client=client):
        return [
            collection.get_document(
                collection="Data", client=client, graph_name="project_names"
            )["filters"]
        ]

    @app.callback(
        [
            Output(f"year-dropdown-{client}", "options"),
            Output(f"year-dropdown-{client}", "value"),
        ],
        [
            Input(f"{client}-overview", "style"),
            Input(f"overview-reset-{client}", "n_clicks"),
        ],
    )
    def load_year_dropdown(dummy_data, reset, client=client):
        return [
            [
                {"label": year, "value": year}
                for year in collection.get_document(
                    collection="Data", client=client, graph_name="List_of_years"
                )
            ],
            str(datetime.now().year),
        ]

    @app.callback(
        Output(f"month-overview-year-{client}", "figure"),
        [Input(f"year-dropdown-{client}", "value")],
    )
    def load_month_overview_per_year(year, client=client):
        if year:
            output = overview_bar_chart.get_fig_new(
                data=fetch_data_for_month_overview(year, client)
            )
            return output
        raise PreventUpdate

    @app.callback(
        Output(f"week-overview-year-{client}", "figure"),
        [Input(f"year-dropdown-{client}", "value")],
    )
    def load_week_overview_per_year(year, client=client):
        if year:
            output = overview_bar_chart.get_fig_new(
                data=fetch_data_for_week_overview(year, client)
            )
            return output
        raise PreventUpdate

    @app.callback(
        Output(f"project-performance-year-{client}", "figure"),
        [Input(f"year-dropdown-{client}", "value")],
    )
    def load_performance_graph_per_year(year, client=client):
        if year:
            output = performance_chart.get_fig(
                fetch_data_for_performance_graph(client=client)
            )
            return output
        raise PreventUpdate

    @app.callback(
        Output(f"pie_chart_overview-year_{client}", "figure"),
        [
            Input(f"week-overview-year-{client}", "clickData"),
            Input(f"month-overview-year-{client}", "clickData"),
            Input(f"overview-reset-{client}", "n_clicks"),
            Input(f"year-dropdown-{client}", "value"),
        ],
    )
    def update_redenna_overview_graph(
        week_click_data, month_click_data, reset, year, client=client
    ):
        """
        This function returns the "Opgegeven reden na" pie chart, based on what the user has clicked on.
        If no input is given, an annual overview is returned. With input, a monthly or weekly view is returned.

        :return: This function returns a pie chart figure.
        """
        if year:
            ctx = dash.callback_context
            data, title = fetch_data_for_redenna_overview(ctx, year, client)
            return redenna_overview_chart.get_fig(data, title)

        raise PreventUpdate

    @app.callback(
        Output(f"FTU_table_c_{client}", "children"),
        [Input(f"year-dropdown-{client}", "value")],
    )
    def load_project_info_table(year, client=client):
        if year:
            return project_info_table.get_ftu_table(
                fetch_data_for_project_info_table(client)
            )
        raise PreventUpdate

    @app.callback(
        [Output(f"status-count-filter-{client}", "data")],
        [
            Input(f"status-counts-laagbouw-{client}", "clickData"),
            Input(f"status-counts-hoogbouw-{client}", "clickData"),
            Input(f"overview-reset-{client}", "n_clicks"),
        ],
        [State(f"status-count-filter-{client}", "data")],
    )
    def set_status_click_filter(_, __, ___, click_filter):

        button_name = list(dash.callback_context.inputs.keys())[2]
        trigger = dash.callback_context.triggered[0]
        if not trigger["value"]:
            raise PreventUpdate

        if trigger["prop_id"] == button_name:  # indicates that reset button is used
            click_filter = {}
        else:
            category, _, cat_filter = trigger["value"]["points"][0][
                "customdata"
            ].partition(";")
            click_filter[category] = cat_filter

        return [click_filter]

    @app.callback(
        [
            Output(f"status-counts-laagbouw-{client}", "figure"),
            Output(f"status-counts-hoogbouw-{client}", "figure"),
        ],
        [
            Input(f"status-count-filter-{client}", "data"),
            Input(f"project-dropdown-{client}", "value"),
        ],
    )
    def update_graphs_using_status_clicks(click_filter, project_name, client=client):
        if not project_name:
            raise PreventUpdate

        data_laagbouw, data_hoogbouw = fetch_data_for_status_barchart(
            project_name, click_filter=click_filter, client=client
        )

        if not data_laagbouw.empty:
            laagbouw = completed_status_counts_bar.get_fig(
                data_laagbouw, title="Status oplevering per fase (LB)"
            )
        else:
            laagbouw = no_graph()

        if not data_hoogbouw.empty:
            hoogbouw = completed_status_counts_bar.get_fig(
                data_hoogbouw, title="Status oplevering per fase (HB & Duplex)"
            )
        else:
            hoogbouw = no_graph()

        return laagbouw, hoogbouw

    @app.callback(
        [
            Output(f"redenna_project_{client}", "figure"),
        ],
        [
            Input(f"status-count-filter-{client}", "data"),
            Input(f"project-dropdown-{client}", "value"),
        ],
    )
    def update_redenna_status_clicks(click_filter, project_name, client=client):
        if not project_name:
            raise PreventUpdate

        total, laagbouw, hoogbouw = fetch_data_for_status_redenna_piechart(
            project_name, click_filter=click_filter, client=client
        )

        if not total.empty:
            fig = redenna_status_pie.get_fig(
                total, laagbouw, hoogbouw, title="Opgegeven reden na"
            )
        else:
            fig = no_graph()

        return [fig]

    @app.callback(
        [
            Output(f"info-container-year-{client}", "children"),
            Output(f"info-container2-year-{client}", "children"),
        ],
        [Input(f"year-dropdown-{client}", "value")],
    )
    def load_global_info_per_year(year, client=client):
        if not year:
            raise PreventUpdate

        output = global_info_list(
            items=fetch_data_for_overview_boxes(client, year) + [{}]
        )

        return [output[0:7], output[7:]]
