import pandas as pd
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

from app import app
from data import collection
from data.data import fetch_data_for_indicator_boxes
from layout.components.list_of_boxes import project_indicator_list

client = "dfn"


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
        Output(f"graph_prog-{client}", "figure"),
    ],
    [
        Input(f"project-dropdown-{client}", "value"),
    ],
)
def update_prognose_graph(drop_selectie):
    if drop_selectie is None:
        raise PreventUpdate

    fig_prog = collection.get_graph(
        client="dfn", graph_name="prognose_graph_dict", project=drop_selectie
    )
    for i, item in enumerate(fig_prog["data"]):
        fig_prog["data"][i]["x"] = pd.to_datetime(item["x"])

    return [fig_prog]
