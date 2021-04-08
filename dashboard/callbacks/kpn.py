import pandas as pd
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

from app import app, toggles
# update value dropdown given selection in scatter chart
from data import collection
from data.data import fetch_data_for_indicator_boxes
from layout.components.indicator import indicator
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

    if toggles.transform_frontend_newindicator:
        indicator_info = project_indicator_list(
            fetch_data_for_indicator_boxes(project=dropdown_selection, client=client)
        )
    else:
        indicator_types = [
            "lastweek_realisatie",
            "weekrealisatie",
            "last_week_bis_realisatie",
            "week_bis_realisatie",
            "weekHCHPend",
        ]
        indicators = collection.get_document(
            collection="Data",
            graph_name="project_indicators",
            project=dropdown_selection,
            client=client,
        )
        indicator_info = [
            indicator(
                value=indicators[el]["counts"],
                previous_value=indicators[el].get("counts_prev"),
                title=indicators[el]["title"],
                sub_title=indicators[el]["subtitle"],
                font_color=indicators[el]["font_color"],
                gauge=indicators[el].get("gauge"),
            )
            for el in indicator_types
        ]

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

    fig_prog = collection.get_graph(
        client="kpn", graph_name="prognose_graph_dict", project=drop_selectie
    )
    for i, item in enumerate(fig_prog["data"]):
        fig_prog["data"][i]["x"] = pd.to_datetime(item["x"])

    return [fig_prog]
