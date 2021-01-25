from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
from google.cloud import firestore

from layout.components.indicator import indicator

import pandas as pd

from app import app

# update value dropdown given selection in scatter chart
from data import collection

client = 'kpn'


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

    indicator_types = ['lastweek_realisatie', 'weekrealisatie', 'weekHCHPend', 'weeknerr']
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
        Output("overzicht_button", 'n_clicks'),
    ],
    [
        Input(f'project-dropdown-{client}', 'value'),
    ],
)
def update_overzicht_button(drop_selectie):
    if drop_selectie is None:
        raise PreventUpdate

    return [-1]


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

    fig_prog = collection.get_graph(client="kpn", graph_name="prognose_graph_dict", project=drop_selectie)
    for i, item in enumerate(fig_prog['data']):
        fig_prog['data'][i]['x'] = pd.to_datetime(item['x'])

    return [fig_prog]


# update FTU table for editing
@app.callback(
    [
        Output(f'table_FTU_{client}', 'editable'),
    ],
    [
        Input('ww', 'value'),
    ],
)
def FTU_table_editable(ww):
    return [ww == 'FttX']


# update firestore given edit FTU table
@app.callback(
    [
        Output(f'project-performance-{client}', 'figure'),
    ],
    [
        Input(f'table_FTU_{client}', 'data'),
    ],
)
def FTU_update(data):
    print('start updating FTU tabel')
    record = dict(graph_name='project_dates', client=client)
    df = pd.DataFrame(data)
    updated_dict = {}
    for col in df:
        if col != "Project":
            updated_dict[col] = dict(zip(df['Project'], df[col]))
    record['record'] = updated_dict
    print(record)
    firestore.Client().collection('Data').document(f'{client}_project_dates').set(record)
    output = collection.get_graph(client=client, graph_name='project_performance')
    return [output]
