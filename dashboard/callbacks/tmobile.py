import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from app import app
from data.data import (fetch_data_for_indicator_boxes_tmobile,
                       fetch_data_for_redenna_modal)
from layout.components.figure import figure
from layout.components.graphs import redenna_modal_chart
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
    project,
):

    changed_id = [p["prop_id"] for p in dash.callback_context.triggered][0]

    if "indicator" in changed_id and (
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

        figure = redenna_modal_chart.get_fig(
            data=fetch_data_for_redenna_modal(
                project=project,
                client=client,
                indicator_type=order_type,
                wait_category=wait_category,
            ),
            title="Reden na",
        )

        output = [
            not is_open,
            figure,
            f"/dash/order_wait_download?project={project}&wait_category={wait_category}&order_type={order_type}",
        ]
    elif close_clicks:
        output = [not is_open, {"data": None, "layout": None}, ""]
    else:
        output = [is_open, {"data": None, "layout": None}, ""]

    return output


@app.callback(
    [
        Output(f"indicators-{client}", "children"),
    ],
    [Input(f"project-dropdown-{client}", "value")],
)
def update_indicators(dropdown_selection):
    if dropdown_selection is None:
        raise PreventUpdate

    data1, data2 = fetch_data_for_indicator_boxes_tmobile(
        project=dropdown_selection, client=client
    )
    out = [
        html.Div(children=project_indicator_list(data1), className="container-display"),
        html.Div(children=project_indicator_list(data2), className="container-display"),
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

    return [out]
