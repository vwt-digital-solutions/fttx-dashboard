# from analyse_dashboard.analyse.functions import graph_overview, update_y_prog_l, targets
# from analyse_dashboard.analyse.functions import performance_matrix, prognose_graph
# from analyse_dashboard.analyse.functions import info_table, overview
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
from google.cloud import firestore

from layout.components.indicator import indicator

import pandas as pd
# import numpy as np

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

    indicator_types = ['weektarget', 'weekrealisatie', 'vorigeweekrealisatie', 'weekHCHPend', 'weeknerr']
    indicators = collection.get_document(collection="Data",
                                         graph_name="project_indicators",
                                         project=dropdown_selection,
                                         client=client)
    indicator_info = [indicator(value=indicators[el]['counts'],
                                previous_value=indicators[el]['counts_prev'],
                                title=indicators[el]['title'],
                                sub_title=indicators[el]['subtitle'],
                                font_color=indicators[el]['font_color']) for el in indicator_types]

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
    return [ww == 'Wout']


# update firestore given edit FTU table
@app.callback(
    [
        # Output('info_globaal_container0_text', 'children'),
        # Output('info_globaal_container1_text', 'children'),
        # Output('info_globaal_container2_text', 'children'),
        # Output('info_globaal_container3_text', 'children'),
        # Output('info_globaal_container4_text', 'children'),
        # Output('info_globaal_container5_text', 'children'),
        # Output('graph_targets_M', 'figure'),
        # Output('graph_targets_W', 'figure'),
        Output('project-performance-kpn', 'figure'),
    ],
    [
        Input('table_FTU_kpn', 'data'),
    ],
)
def FTU_update(data):
    record = dict(id='analysis')
    FTU0 = {}
    FTU1 = {}
    for el in data:
        FTU0[el['Project']] = el['FTU0']
        FTU1[el['Project']] = el['FTU1']
    record['FTU0'] = FTU0
    record['FTU1'] = FTU1
    firestore.Client().collection('Data').document(record['id']).set(record)

    # # to update overview graphs:
    # doc = firestore.Client().collection('Data').document('analysis2').get().to_dict()
    # doc2 = firestore.Client().collection('Data').document('analysis3').get().to_dict()
    # x_d = pd.to_datetime(doc['x_d'])
    # tot_l = doc['tot_l']
    # HP = doc['HP']
    # HC_HPend_l = doc['HC_HPend_l']
    # Schouw_BIS = doc['Schouw_BIS']
    # # HAS_werkvoorraad = doc['HAS_werkvoorraad']
    # HPend_l = doc['HPend_l']
    # d_real_l = doc2['d_real_l']
    # d_real_li = doc2['d_real_li']
    # y_prog_l = doc['y_prog_l']
    # y_target_l = doc['y_target_l']
    # rc1 = doc['rc1']
    # rc2 = doc['rc2']
    # t_shift = doc['t_shift']
    # cutoff = doc['cutoff']
    # y_voorraad_act = doc['y_voorraad_act']
    # x_prog = np.array(doc['x_prog'])
    # n_err = doc2['n_err']
    # for key in y_prog_l:
    #     y_prog_l[key] = np.array(y_prog_l[key])
    #     y_target_l[key] = np.array(y_target_l[key])
    #     t_shift[key] = int(t_shift[key])
    #     if key in rc1:
    #         rc1[key] = np.array(rc1[key])
    #     if key in rc2:
    #         rc2[key] = np.array(rc2[key])
    #     if key in d_real_l:
    #         d_real_l[key] = pd.DataFrame(columns=['Aantal'], index=d_real_li[key], data=d_real_l[key])
    # y_prog_l, _ = update_y_prog_l(FTU0, d_real_l, t_shift, rc1, rc2, y_prog_l, x_d, x_prog, cutoff)
    # y_target_l, t_diff = targets(x_prog, x_d, t_shift, FTU0, FTU1, rc1, d_real_l)
    # df_prog, df_target, df_real, df_plan = overview(x_d, y_prog_l, tot_l, d_real_l, HP, y_target_l)

    # # record, data_pr_w, data_t_w, data_r_w, data_p_w = graph_overview(
    #   df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad, res='W-MON')  # 2019-12-30 -- 2020-12-21
    # record, jaaroverzicht, data_pr_m, data_t_m, data_r_m, data_p_m = graph_overview(
    #   df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad, res='M')  # 2019-12-30 -- 2020-12-21
    # # record = performance_matrix(x_d, y_target_l, d_real_l, tot_l, t_diff, y_voorraad_act)
    # # record_dict = prognose_graph(x_d, y_prog_l, d_real_l, y_target_l)
    # # record = info_table(tot_l, d_real_l, HP, y_target_l, x_d, HC_HPend_l, Schouw_BIS, HPend_l, n_err)

    # HC_HPend = jaaroverzicht['HC_HPend']
    # HAS_werkvoorraad = jaaroverzicht["HAS_werkvoorraad"]

    # out0 = 'HPend afgesproken: ' + jaaroverzicht['target']
    # out1 = 'HPend gerealiseerd: ' + jaaroverzicht['real']
    # out2 = 'HPend gepland vanaf nu: ' + jaaroverzicht['plan']
    # out3 = 'HPend voorspeld vanaf nu: ' + jaaroverzicht['prog']
    # out4 = jaaroverzicht['HC_HPend']
    # out5 = HAS_werkvoorraad

    # out6 = collection.get_graph(client="kpn", graph_name='graph_targets_M')
    # out7 = collection.get_graph(client="kpn", graph_name='graph_targets_W')
    out8 = collection.get_graph(client="kpn", graph_name='project_performance')

    # return [out0, out1, out2, out3, out4, out5, out6, out7, out8]
    return [out8]
