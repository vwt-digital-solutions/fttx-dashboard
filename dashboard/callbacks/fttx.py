from datetime import datetime
from urllib.parse import urlencode

import dash
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

import config
from app import app, toggles
from config import colors_vwt as colors
from data import collection
from data.data import (completed_status_counts, fetch_data_for_month_overview,
                       fetch_data_for_overview_boxes,
                       fetch_data_for_overview_graphs,
                       fetch_data_for_performance_graph,
                       fetch_data_for_project_info_table,
                       fetch_data_for_redenna_overview,
                       fetch_data_for_week_overview,
                       redenna_by_completed_status)
from layout.components import redenna_status_pie
from layout.components.global_info_list_old import global_info_list_old
from layout.components.graphs import (completed_status_counts_bar,
                                      overview_bar_chart, performance_chart,
                                      project_info_table,
                                      redenna_overview_chart)
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
            if toggles.transform_frontend_newindicator:
                output = overview_bar_chart.get_fig_new(
                    data=fetch_data_for_month_overview(year, client)
                )
            else:
                output = overview_bar_chart.get_fig(
                    data=fetch_data_for_overview_graphs(
                        year=year, freq="M", period="month", client=client
                    ),
                    year=year,
                )
            return output
        raise PreventUpdate

    @app.callback(
        Output(f"week-overview-year-{client}", "figure"),
        [Input(f"year-dropdown-{client}", "value")],
    )
    def load_week_overview_per_year(year, client=client):
        if year:
            if toggles.transform_frontend_newindicator:
                output = overview_bar_chart.get_fig_new(
                    data=fetch_data_for_week_overview(year, client)
                )
            else:
                output = overview_bar_chart.get_fig(
                    data=fetch_data_for_overview_graphs(
                        year=year, freq="W-MON", period="week", client=client
                    ),
                    year=year,
                )
            return output
        raise PreventUpdate

    @app.callback(
        Output(f"project-performance-year-{client}", "figure"),
        [Input(f"year-dropdown-{client}", "value")],
    )
    def load_performance_graph_per_year(year, client=client):
        if year:
            if toggles.transform_frontend_newindicator:
                output = performance_chart.get_fig(
                    fetch_data_for_performance_graph(year=year, client=client)
                )
            else:
                output = collection.get_graph(
                    client=client, graph_name="project_performance"
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
    def set_status_click_filter(
        laagbouw_click, hoogbouw_click, reset_button, click_filter
    ):
        ctx = dash.callback_context
        if not click_filter:
            click_filter = {}
        if isinstance(click_filter, list):
            click_filter = click_filter[0]
        if ctx.triggered:
            for trigger in ctx.triggered:
                if trigger["prop_id"] == list(ctx.inputs.keys())[2]:
                    return [None]

                for point in trigger["value"]["points"]:
                    category, _, cat_filter = point["customdata"].partition(";")
                    click_filter[category] = cat_filter
                    return [click_filter]

    @app.callback(
        [
            Output(f"status-counts-laagbouw-{client}", "figure"),
            Output(f"status-counts-hoogbouw-{client}", "figure"),
            Output(f"status-counts-laagbouw-{client}-container", "style"),
            Output(f"status-counts-hoogbouw-{client}-container", "style"),
        ],
        [
            Input(f"status-count-filter-{client}", "data"),
            Input(f"project-dropdown-{client}", "value"),
        ],
    )
    def update_graphs_using_status_clicks(click_filter, project_name, client=client):
        if project_name:
            status_counts = completed_status_counts(
                project_name, click_filter=click_filter, client=client
            )
            if status_counts is None:
                return [
                    no_graph(),
                    no_graph(),
                    {"display": "block"},
                    {"display": "block"},
                ]
            laagbouw = completed_status_counts_bar.get_fig(
                status_counts.laagbouw, title="Status oplevering per fase (LB)"
            )
            laagbouw_style = {"display": "block"} if laagbouw else {"display": "none"}
            hoogbouw = completed_status_counts_bar.get_fig(
                status_counts.hoogbouw, title="Status oplevering per fase (HB & Duplex)"
            )
            hoogbouw_style = {"display": "block"} if hoogbouw else {"display": "none"}
            return laagbouw, hoogbouw, laagbouw_style, hoogbouw_style
        return (
            {"data": None, "layout": None},
            {"data": None, "layout": None},
            {"display": "block"},
            {"display": "block"},
        )

    @app.callback(
        [
            Output(f"redenna_project_{client}", "figure"),
            Output(f"project-redenna-download-{client}", "href"),
        ],
        [
            Input(f"status-count-filter-{client}", "data"),
            Input(f"project-dropdown-{client}", "value"),
        ],
    )
    def update_redenna_status_clicks(click_filter, project_name, client=client):
        if project_name:
            redenna_counts = redenna_by_completed_status(
                project_name, click_filter=click_filter, client=client
            )
            if redenna_counts is None:
                return [no_graph(), ""]
            redenna_pie = redenna_status_pie.get_fig(
                redenna_counts,
                title="Opgegeven reden na",
                colors=[
                    colors["green"],
                    colors["yellow"],
                    colors["red"],
                    colors["vwt_blue"],
                ],
            )
            if click_filter:
                download_url = f"/dash/project_redenna_download?project={project_name}&{urlencode(click_filter)}"
            else:
                download_url = f"/dash/project_redenna_download?project={project_name}"

            return [redenna_pie, download_url]
        return [{"data": None, "layout": None}, ""]

    @app.callback(
        Output(f"info-container-year-{client}", "children"),
        [Input(f"year-dropdown-{client}", "value")],
    )
    def load_global_info_per_year(year, client=client):
        if not year:
            raise PreventUpdate

        if toggles.transform_frontend_newindicator:
            output = global_info_list(
                className="container-display",
                items=fetch_data_for_overview_boxes(client, year),
            )

        elif toggles.overview_indicators:
            parameters_global_info_list = [
                dict(
                    id_="info_globaal_container0",
                    title="Internal Target",
                    text1="HPciviel: ",
                    value1=str(
                        int(
                            collection.get_document(
                                collection="Data",
                                graph_name="target_intern_bis",
                                client=client,
                                year=year,
                                frequency="Y",
                            )
                        )
                    ),
                    text2="HPend: ",
                    value2=str(
                        int(
                            collection.get_document(
                                collection="Data",
                                graph_name="target",
                                client=client,
                                year=year,
                                frequency="Y",
                            )
                        )
                    ),
                ),
                dict(
                    id_="info_globaal_container0",
                    title="Client Target",
                    text1="HPciviel: ",
                    value1=str(
                        int(
                            collection.get_document(
                                collection="Data",
                                graph_name="bis_target_client",
                                client=client,
                                year=year,
                                frequency="Y",
                            )
                        )
                    ),
                    text2="HPend: ",
                    value2=str(
                        int(
                            collection.get_document(
                                collection="Data",
                                graph_name="has_target_client",
                                client=client,
                                year=year,
                                frequency="Y",
                            )
                        )
                    ),
                ),
                dict(
                    id_="info_globaal_container1",
                    title="Realisatie",
                    text1="HPciviel: ",
                    value1=str(
                        collection.get_document(
                            collection="Data",
                            graph_name="realisatie_bis",
                            client=client,
                            year=year,
                            frequency="Y",
                        )
                    ),
                    text2="HPend: ",
                    value2=str(
                        collection.get_document(
                            collection="Data",
                            graph_name="realisatie_hpend",
                            client=client,
                            year=year,
                            frequency="Y",
                        )
                    ),
                ),
                dict(
                    id_="info_globaal_container2",
                    title="Planning",
                    text1="HPciviel: ",
                    value1="n.v.t.",
                    text2="HPend: ",
                    value2=str(
                        int(
                            collection.get_document(
                                collection="Data",
                                graph_name="planning_minus_HPend",
                                client=client,
                                year=year,
                                frequency="Y",
                            )
                        )
                    )
                    if year == str(datetime.now().year)
                    else "n.v.t.",  # We only show planning for the current year
                ),
                dict(
                    id_="info_globaal_container3",
                    title="Voorspelling",
                    text1="HPciviel: ",
                    value1="n.v.t.",
                    text2="HPend: ",
                    value2=str(
                        int(
                            collection.get_document(
                                collection="Data",
                                graph_name="voorspelling_minus_HPend",
                                client=client,
                                year=year,
                                frequency="Y",
                            )
                        )
                    )
                    if client != "tmobile" and year == str(datetime.now().year)
                    else "n.v.t."
                    # We only show voorspelling for the current year and only for KPN and DFN
                ),
                dict(
                    id_="info_globaal_container5",
                    title="Werkvoorraad",
                    text1="HPciviel: ",
                    value1=str(
                        collection.get_document(
                            collection="Data",
                            graph_name="werkvoorraad_bis",
                            client=client,
                            year=year,
                            frequency="Y",
                        )
                    ),
                    text2="HPend: ",
                    value2=str(
                        collection.get_document(
                            collection="Data",
                            graph_name="werkvoorraad_has",
                            client=client,
                            year=year,
                            frequency="Y",
                        )
                    ),
                ),
                dict(
                    id_="info_globaal_container4",
                    title="Actuele HC / HPend",
                    text1="HPciviel: ",
                    value1="n.v.t.",
                    text2="HPend: ",
                    value2=str(
                        format(
                            collection.get_document(
                                collection="Data",
                                graph_name="ratio_hc_hpend",
                                client=client,
                                year=year,
                                frequency="Y",
                            ),
                            ".2f",
                        )
                    )
                    if client != "tmobile"
                    else "n.v.t.",  # We only show HC/HPend for KPN and DFN
                ),
                dict(
                    id_="info_globaal_container4",
                    title="Ratio <12 weken",
                    text1="HPciviel: ",
                    value1="n.v.t.",
                    text2="HPend: ",
                    value2=str(
                        format(
                            collection.get_document(
                                collection="Data",
                                graph_name="ratio_8weeks_hpend",
                                client=client,
                                year=year,
                                frequency="Y",
                            ),
                            ".2f",
                        )
                    )
                    if client == "tmobile"
                    else "n.v.t.",  # We only show Ratio 12 weeks for tmobile
                ),
            ]

            if toggles.leverbetrouwbaarheid:
                value = str(
                    format(
                        collection.get_document(
                            collection="Data",
                            graph_name="ratio_leverbetrouwbaarheid",
                            client=client,
                            year=year,
                            frequency="Y",
                        ),
                        ".2f",
                    )
                )
                parameters_global_info_list += dict(
                    id_="info_globaal_container6",
                    title="Leverbetrouwbaarheid",
                    text=f"Leverbetrouwbaarheid in {year}: ",
                    value=value,
                )
            output = global_info_list(
                items=parameters_global_info_list, className="container-display"
            )
        else:
            parameters_global_info_list = [
                dict(
                    id_="info_globaal_container0",
                    title="Internal Target (HPend)",
                    text=f"HPend afgesproken in {year}: ",
                    value=str(
                        int(
                            collection.get_document(
                                collection="Data",
                                graph_name="target",
                                client=client,
                                year=year,
                                frequency="Y",
                            )
                        )
                    ),
                ),
                dict(
                    id_="info_globaal_container1",
                    title="Realisatie (HPend)",
                    text=f"HPend gerealiseerd in {year}: ",
                    value=str(
                        collection.get_document(
                            collection="Data",
                            graph_name="realisatie_hpend",
                            client=client,
                            year=year,
                            frequency="Y",
                        )
                    ),
                ),
                dict(
                    id_="info_globaal_container1",
                    title="Realisatie (BIS)",
                    text=f"BIS gerealiseerd in {year}: ",
                    value=str(
                        collection.get_document(
                            collection="Data",
                            graph_name="realisatie_bis",
                            client=client,
                            year=year,
                            frequency="Y",
                        )
                    ),
                ),
                dict(
                    id_="info_globaal_container2",
                    title="Planning (VWT)",
                    text="HPend gepland vanaf nu: ",
                    value=str(
                        int(
                            collection.get_document(
                                collection="Data",
                                graph_name="planning_minus_HPend",
                                client=client,
                                year=year,
                                frequency="Y",
                            )
                        )
                    )
                    if year == str(datetime.now().year)
                    else "n.v.t.",  # We only show planning for the current year
                ),
                dict(
                    id_="info_globaal_container3",
                    title="Voorspelling (VQD)",
                    text="HPend voorspeld vanaf nu: ",
                    value=str(
                        int(
                            collection.get_document(
                                collection="Data",
                                graph_name="voorspelling_minus_HPend",
                                client=client,
                                year=year,
                                frequency="Y",
                            )
                        )
                    )
                    if client != "tmobile" and year == str(datetime.now().year)
                    else "n.v.t."
                    # We only show voorspelling for the current year and only for KPN and DFN
                ),
                dict(
                    id_="info_globaal_container5",
                    title="Werkvoorraad HAS",
                    text=f"Werkvoorraad HAS in {year}: ",
                    value=str(
                        collection.get_document(
                            collection="Data",
                            graph_name="werkvoorraad_has",
                            client=client,
                            year=year,
                            frequency="Y",
                        )
                    ),
                ),
                dict(
                    id_="info_globaal_container4",
                    title="Actuele HC / HPend",
                    text=f"HC/HPend in {year}: ",
                    value=str(
                        format(
                            collection.get_document(
                                collection="Data",
                                graph_name="ratio_hc_hpend",
                                client=client,
                                year=year,
                                frequency="Y",
                            ),
                            ".2f",
                        )
                    )
                    if client != "tmobile"
                    else "n.v.t.",  # We only show HC/HPend for KPN and DFN
                ),
                dict(
                    id_="info_globaal_container4",
                    title="Ratio <12 weken",
                    text=f"Ratio <12 weken in {year}: ",
                    value=str(
                        format(
                            collection.get_document(
                                collection="Data",
                                graph_name="ratio_8weeks_hpend",
                                client=client,
                                year=year,
                                frequency="Y",
                            ),
                            ".2f",
                        )
                    )
                    if client == "tmobile"
                    else "n.v.t.",  # We only show Ratio <12 weeks for tmobile
                ),
                dict(
                    id_="info_globaal_container6",
                    title="Leverbetrouwbaarheid",
                    text=f"Leverbetrouwbaarheid in {year}: ",
                    value=str(
                        format(
                            collection.get_document(
                                collection="Data",
                                graph_name="ratio_leverbetrouwbaarheid",
                                client=client,
                                year=year,
                                frequency="Y",
                            ),
                            ".2f",
                        )
                    )
                    if toggles.leverbetrouwbaarheid
                    else "n.v.t.",
                ),
            ]
            output = global_info_list_old(
                items=parameters_global_info_list, className="container-display"
            )

        return [output]
