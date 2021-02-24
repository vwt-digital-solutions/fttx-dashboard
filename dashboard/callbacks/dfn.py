# from analyse_dashboard.analyse.functions import graph_overview, update_y_prog_l, targets
# from analyse_dashboard.analyse.functions import performance_matrix, prognose_graph
# from analyse_dashboard.analyse.functions import info_table, overview

import pandas as pd
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

from app import app
from app import toggles
# update value dropdown given selection in scatter chart
from data import collection
from layout.components.indicator import indicator

client = 'dfn'


@app.callback(
    [
        Output(f"indicators-{client}", 'children'),
    ],
    [
        Input(f'project-dropdown-{client}', 'value'),
    ],
)
def update_indicators(dropdown_selection):
    if dropdown_selection is None:
        raise PreventUpdate

    if toggles.project_bis:
        indicator_types = ['lastweek_realisatie', 'weekrealisatie', 'last_week_bis_realisatie', 'week_bis_realisatie',
                           'weekHCHPend']
    else:
        indicator_types = ['lastweek_realisatie', 'weekrealisatie', 'weekHCHPend']
    indicators = collection.get_document(collection="Data",
                                         graph_name="project_indicators",
                                         project=dropdown_selection,
                                         client=client)
    indicator_info = [indicator(value=indicators[el]['counts'],
                                previous_value=indicators[el].get('counts_prev'),
                                title=indicators[el]['title'],
                                sub_title=indicators[el]['subtitle'],
                                font_color=indicators[el]['font_color'],
                                gauge=indicators[el].get("gauge")) for el in indicator_types]

    return [indicator_info]


@app.callback(
    [
        Output(f"graph_prog-{client}", 'figure'),
    ],
    [
        Input(f'project-dropdown-{client}', 'value'),
    ],
)
def update_prognose_graph(drop_selectie):
    if drop_selectie is None:
        raise PreventUpdate

    fig_prog = collection.get_graph(client="dfn", graph_name="prognose_graph_dict", project=drop_selectie)
    for i, item in enumerate(fig_prog['data']):
        fig_prog['data'][i]['x'] = pd.to_datetime(item['x'])

    return [fig_prog]
