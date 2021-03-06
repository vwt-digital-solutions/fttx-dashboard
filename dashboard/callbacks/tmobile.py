import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from app import app, toggles
from config import colors_vwt as colors
from data import collection
from data.data import (fetch_data_for_indicator_boxes_tmobile,
                       fetch_data_for_redenna_modal, fetch_sleutels_figure)
from layout.components.figure import figure
from layout.components.graphs import pie_chart, redenna_modal_chart
from layout.components.indicator import indicator
from layout.components.list_of_boxes import project_indicator_list

client = "tmobile"


@app.callback(
    [
        Output("modal-sm", "is_open"),
        Output(f"indicator-modal-{client}", "figure"),
        Output(f"indicator-download-{client}", "href"),
    ],
    [
        Input(f"indicator-too_late-hc_aanleg-{client}", "n_clicks"),
        Input(f"indicator-late-hc_aanleg-{client}", "n_clicks"),
        Input(f"indicator-on_time-hc_aanleg-{client}", "n_clicks"),
        Input(f"indicator-too_late-patch_only-{client}", "n_clicks"),
        Input(f"indicator-late-patch_only-{client}", "n_clicks"),
        Input(f"indicator-on_time-patch_only-{client}", "n_clicks"),
        Input("close-sm", "n_clicks"),
    ],
    [
        State("modal-sm", "is_open"),
        State(f"indicator-data-{client}", "data"),
        State(f"project-dropdown-{client}", "value"),
    ],
)
def indicator_modal(
    too_late_clicks_hc,
    late_clicks_hc,
    on_time_clicks_hc,
    too_late_clicks_po,
    late_clicks_po,
    on_time_clicks_po,
    close_clicks,
    is_open,
    result,
    project,
):

    changed_id = [p["prop_id"] for p in dash.callback_context.triggered][0]

    if close_clicks:
        output = [not is_open, {"data": None, "layout": None}, ""]

    elif "indicator" in changed_id and (
        too_late_clicks_hc
        or late_clicks_hc
        or on_time_clicks_hc
        or too_late_clicks_po
        or late_clicks_po
        or on_time_clicks_po
    ):
        changed_list = changed_id.split("-")
        wait_category = changed_list[1]
        order_type = changed_list[2]

        if toggles.transform_frontend_newindicator:
            figure = redenna_modal_chart.get_fig(
                data=fetch_data_for_redenna_modal(
                    project=project,
                    client=client,
                    indicator_type=order_type,
                    wait_category=wait_category,
                ),
                title="Reden na",
            )
            sleutels_figure = fetch_sleutels_figure(
                project=project,
                client=client,
                indicator_type=order_type,
                wait_category=wait_category,
            )
        else:
            cluster_redenna_sorted_dict = dict(
                sorted(
                    result[wait_category + "-" + order_type]["cluster_redenna"].items()
                )
            )
            # hier make figure functie...
            figure = pie_chart.get_html(
                labels=list(cluster_redenna_sorted_dict.keys()),
                values=list(cluster_redenna_sorted_dict.values()),
                title="Reden na",
                colors=[
                    colors["green"],
                    colors["yellow"],
                    colors["red"],
                    colors["vwt_blue"],
                ],
            )

        output = [
            not is_open,
            figure,
            f"/dash/order_wait_download?sleutels={sleutels_figure}&project={project}&wait_category={wait_category}",
        ]

    else:
        output = [is_open, {"data": None, "layout": None}, ""]

    return output


@app.callback(
    [
        Output(f"indicators-{client}", "children"),
        Output(f"indicator-data-{client}", "data"),
    ],
    [Input(f"project-dropdown-{client}", "value")],
)
def update_indicators(dropdown_selection):
    if dropdown_selection is None:
        raise PreventUpdate

    if toggles.transform_frontend_newindicator:
        data1, data2 = fetch_data_for_indicator_boxes_tmobile(
            project=dropdown_selection, client=client
        )
        out = [
            html.Div(
                children=project_indicator_list(data1), className="container-display"
            ),
            html.Div(
                children=project_indicator_list(data2), className="container-display"
            ),
        ]

        out = out + [
            dbc.Modal(
                [
                    dbc.ModalBody(
                        figure(
                            graph_id=f"indicator-modal-{client}",
                            className="",
                            figure={"data": None, "layout": None},
                        )
                    ),
                    dbc.ModalFooter(
                        children=[
                            html.A(
                                dbc.Button(
                                    "Download",
                                    id="download-indicator",
                                    className="ml-auto",
                                ),
                                id=f"indicator-download-{client}",
                                href="/dash/urlToDownload",
                            ),
                            dbc.Button("Close", id="close-sm", className="ml-auto"),
                        ]
                    ),
                ],
                id="modal-sm",
                size="lg",
                centered=True,
            )
        ]

        # this amd its output can be removed together with the toggle
        indicators = collection.get_document(
            collection="Data",
            graph_name="project_indicators",
            project=dropdown_selection,
            client=client,
        )

    else:
        indicators_row1 = [
            "on_time-hc_aanleg",
            "limited-hc_aanleg",
            "late-hc_aanleg",
            "ratio",
        ]
        indicators_row2 = [
            "on_time-patch_only",
            "limited-patch_only",
            "late-patch_only",
            "ready_for_has",
        ]
        indicators = collection.get_document(
            collection="Data",
            graph_name="project_indicators",
            project=dropdown_selection,
            client=client,
        )

        out = [
            html.Div(
                children=[
                    indicator(
                        value=indicators[element]["counts"],
                        previous_value=indicators[element].get("counts_prev", None),
                        title=indicators[element]["title"],
                        sub_title=indicators[element].get("subtitle", " "),
                        font_color=indicators[element].get("font_color", "black"),
                        invert_delta=indicators[element].get("invert_delta", True),
                        percentage=indicators[element].get("percentage"),
                        id=f"indicator-{element}-{client}",
                    )
                    for element in indicators_row1
                ],
                className="container-display",
            ),
            html.Div(
                children=[
                    indicator(
                        value=indicators[element]["counts"],
                        previous_value=indicators[element].get("counts_prev", None),
                        title=indicators[element]["title"],
                        sub_title=indicators[element].get("subtitle", " "),
                        font_color=indicators[element].get("font_color", "black"),
                        invert_delta=indicators[element].get("invert_delta", True),
                        percentage=indicators[element].get("percentage"),
                        id=f"indicator-{element}-{client}",
                    )
                    for element in indicators_row2
                ],
                className="container-display",
            ),
        ]

        out = out + [
            dbc.Modal(
                [
                    dbc.ModalBody(
                        figure(
                            graph_id=f"indicator-modal-{client}",
                            className="",
                            figure={"data": None, "layout": None},
                        )
                    ),
                    dbc.ModalFooter(
                        children=[
                            html.A(
                                dbc.Button(
                                    "Download",
                                    id="download-indicator",
                                    className="ml-auto",
                                ),
                                id=f"indicator-download-{client}",
                                href="/dash/urlToDownload",
                            ),
                            dbc.Button("Close", id="close-sm", className="ml-auto"),
                        ]
                    ),
                ],
                id="modal-sm",
                size="lg",
                centered=True,
            )
        ]

    return [out, indicators]
