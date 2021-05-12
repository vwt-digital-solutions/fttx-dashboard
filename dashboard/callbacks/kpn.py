from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

from app import app
from data.data import (fetch_data_for_indicator_boxes,
                       fetch_data_for_progress_HPend_chart)
from layout.components.graphs import progress_HPend_chart
from layout.components.list_of_boxes import project_indicator_list

client = "kpn"


@app.callback(
    [
        Output(f"indicators-{client}", "children"),
    ],
    [
        Input(f"project-dropdown-{client}", "value"),
    ],
)
def update_indicators(dropdown_selection):
    if dropdown_selection is None:
        raise PreventUpdate

    indicator_info = project_indicator_list(
        fetch_data_for_indicator_boxes(project=dropdown_selection, client=client)
    )
    return [indicator_info]


@app.callback(
    [
        Output("overzicht_button", "n_clicks"),
    ],
    [
        Input(f"project-dropdown-{client}", "value"),
    ],
)
def update_overzicht_button(drop_selectie):
    if drop_selectie is None:
        raise PreventUpdate

    return [-1]


@app.callback(
    [
        Output(f"graph_prog-{client}", "figure"),
    ],
    [
        Input(f"project-dropdown-{client}", "value"),
    ],
)
def update_prognose_graph(drop_selectie):
    if drop_selectie is None:
        raise PreventUpdate

    fig_prog = progress_HPend_chart.get_fig(
        fetch_data_for_progress_HPend_chart(client=client, project=drop_selectie)
    )

    return [fig_prog]
