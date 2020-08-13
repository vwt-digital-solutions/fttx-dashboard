from analyse_dashboard.analyse.functions import graph_overview, update_y_prog_l, targets
from analyse_dashboard.analyse.functions import performance_matrix, prognose_graph
from analyse_dashboard.analyse.functions import info_table, overview

from data import api
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from google.cloud import firestore

import pandas as pd
import numpy as np

from app import app

# update value dropdown given selection in scatter chart
from data.graph import graph
from data.figure import figure_data
from data.jaaroverzicht import jaaroverzicht_data


@app.callback(
    [Output("project-dropdown", 'value'),
     ],
    [Input("project_performance", 'clickData'),
     ]
)
def update_dropdown(value):
    print(value)
    return [value['points'][0]['text']]


# update graphs
@app.callback(
    [
        Output("graph_targets_overall_c", 'hidden'),
        Output("graph_targets_overallM_c", 'hidden'),
        Output("info_globaal_container0", 'hidden'),
        Output("info_globaal_container1", 'hidden'),
        Output("info_globaal_container2", 'hidden'),
        Output("info_globaal_container3", 'hidden'),
        Output("info_globaal_container4", 'hidden'),
        Output("info_globaal_container5", 'hidden'),
        Output("graph_speed_c", 'hidden'),
        Output("ww_c", 'hidden'),
        Output('FTU_table_c', 'hidden'),
        Output("graph_prog_c", "hidden"),
        Output("table_info", "hidden"),
        Output("Bar_LB_c", "hidden"),
        Output("Bar_HB_c", "hidden"),
        Output("Pie_NA_oid", "hidden"),
        Output("Pie_NA_cid", "hidden"),
        Output("geo_plot", 'figure'),
        Output("table_c", 'children'),
        Output("geo_plot_c", "hidden"),
        Output("table_c", "hidden"),
    ],
    [
        Input("overzicht_button", 'n_clicks'),
        Input("detail_button", "n_clicks")
    ],
    [
        State('project-dropdown', 'value'),
        State("aggregate_data", 'data'),
    ],
)
def update_graphs(n_o, n_d, drop_selectie, mask_all):
    if drop_selectie is None:
        raise PreventUpdate
    if n_o == -1:
        hidden = True
    else:
        hidden = False
        n_d = 0
    if n_d in [1, 3, 5]:
        hidden1 = False
        fig = graph(7, drop_selectie, mask_all)
    else:
        hidden1 = True
        fig = dict(geo={'data': None, 'layout': dict()}, table=None)
    return [
        hidden,  # graph_targets_overall_c
        hidden,  # graph_targets_overallM_c
        hidden,  # info_globaal_container0
        hidden,  # info_globaal_container1
        hidden,  # info_globaal_container2
        hidden,  # info_globaal_container3
        hidden,  # info_globaal_container4
        hidden,  # info_globaal_container5
        hidden,  # graph_speed_c
        hidden,  # ww_c
        hidden,  # FTU_table_c
        not hidden,  # graph_prog_c
        not hidden,  # table_info
        not hidden,  # Bar_LB_c
        not hidden,  # Bar_HB_c
        hidden,  # Pie_NA_oid
        not hidden,  # Pie_NA_cid
        fig['geo'],  # geo_plot
        fig['table'],  # table_c
        hidden1,  # geo_plot_c
        hidden1  # table_c
    ]


# update middle-top charts given dropdown selection
@app.callback(
    [
        Output("graph_prog", 'figure'),
        Output("table_info", 'children'),
        Output("overzicht_button", 'n_clicks'),
    ],
    [
        Input('project-dropdown', 'value'),
    ],
)
def middle_top_graphs(drop_selectie):
    if drop_selectie is None:
        raise PreventUpdate

    fig_prog = graph(1, drop_selectie, None)
    table_info = graph(8, drop_selectie, None)

    return [fig_prog, table_info, -1]


# update click bar charts
@app.callback(
    [
        Output("Bar_LB", "figure"),
        Output("Bar_HB", "figure"),
        Output("Pie_NA_c", "figure"),
        Output("aggregate_data", 'data'),
        Output("aggregate_data2", 'data'),
        Output("detail_button", "n_clicks")
    ],
    [Input('project-dropdown', 'value'),
     Input("Bar_LB", 'clickData'),
     Input("Bar_HB", 'clickData'),
     ],
    [State("aggregate_data", 'data'),
     State("aggregate_data2", 'data'),
     ]
)
def click_bars(drop_selectie, cell_bar_LB, cell_bar_HB, mask_all, filter_a):
    if drop_selectie is None:
        raise PreventUpdate

    if (drop_selectie == filter_a) & ((cell_bar_LB is not None) | (cell_bar_HB is not None)):
        if cell_bar_LB is not None:
            pt_x = cell_bar_LB['points'][0]['x']
            if cell_bar_LB['points'][0]['curveNumber'] == 0:
                pt_cell = 'LB1'
            if cell_bar_LB['points'][0]['curveNumber'] == 1:
                pt_cell = 'LB1HP'
            if cell_bar_LB['points'][0]['curveNumber'] == 2:
                pt_cell = 'LB0'
        if cell_bar_HB is not None:
            pt_x = cell_bar_HB['points'][0]['x']
            if cell_bar_HB['points'][0]['curveNumber'] == 0:
                pt_cell = 'HB1'
            if cell_bar_HB['points'][0]['curveNumber'] == 1:
                pt_cell = 'HB1HP'
            if cell_bar_HB['points'][0]['curveNumber'] == 2:
                pt_cell = 'HB0'
        mask_all += pt_x + pt_cell

        doc = api.get('/Graphs?id=bar_names')[0]['bar_names']
        if mask_all not in doc:
            mask_all = '0'
    else:
        mask_all = '0'

    barLB = graph(5, drop_selectie, mask_all)
    barHB = graph(6, drop_selectie, mask_all)
    pieNA = graph(10, drop_selectie, mask_all)

    return [barLB, barHB, pieNA, mask_all, drop_selectie, 0]


# update FTU table for editing
@app.callback(
    [
        Output('table_FTU', 'editable'),
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
        Output('info_globaal_container0_text', 'children'),
        Output('info_globaal_container1_text', 'children'),
        Output('info_globaal_container2_text', 'children'),
        Output('info_globaal_container3_text', 'children'),
        Output('info_globaal_container4_text', 'children'),
        Output('graph_targets_ov', 'figure'),
        Output('graph_targets_m', 'figure'),
        Output('project_performance', 'figure'),
        Output('info_globaal_container5_text', 'children'),
    ],
    [
        Input('table_FTU', 'data'),
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
    firestore.Client().collection('Graphs').document(record['id']).set(record)

    # to update overview graphs:
    HC_HPend = firestore.Client().collection('Graphs').document('jaaroverzicht').get().to_dict()['HC_HPend']
    doc = firestore.Client().collection('Graphs').document('analysis2').get().to_dict()
    doc2 = firestore.Client().collection('Graphs').document('analysis3').get().to_dict()
    x_d = pd.to_datetime(doc['x_d'])
    tot_l = doc['tot_l']
    HP = doc['HP']
    HC_HPend_l = doc['HC_HPend_l']
    Schouw_BIS = doc['Schouw_BIS']
    HAS_werkvoorraad = doc['HAS_werkvoorraad']
    HPend_l = doc['HPend_l']
    d_real_l = doc2['d_real_l']
    d_real_li = doc2['d_real_li']
    y_prog_l = doc['y_prog_l']
    y_target_l = doc['y_target_l']
    rc1 = doc['rc1']
    rc2 = doc['rc2']
    t_shift = doc['t_shift']
    cutoff = doc['cutoff']
    y_voorraad_act = doc['y_voorraad_act']
    x_prog = np.array(doc['x_prog'])
    n_err = doc2['n_err']
    for key in y_prog_l:
        y_prog_l[key] = np.array(y_prog_l[key])
        y_target_l[key] = np.array(y_target_l[key])
        t_shift[key] = int(t_shift[key])
        if key in rc1:
            rc1[key] = np.array(rc1[key])
        if key in rc2:
            rc2[key] = np.array(rc2[key])
        if key in d_real_l:
            d_real_l[key] = pd.DataFrame(columns=['Aantal'], index=d_real_li[key], data=d_real_l[key])
    y_prog_l, _ = update_y_prog_l(FTU0, d_real_l, t_shift, rc1, rc2, y_prog_l, x_d, x_prog, cutoff)
    y_target_l, t_diff = targets(x_prog, x_d, t_shift, FTU0, FTU1, rc1, d_real_l)

    df_prog, df_target, df_real, df_plan = overview(x_d, y_prog_l, tot_l, d_real_l, HP, y_target_l)
    graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad,
                   res='W-MON')  # 2019-12-30 -- 2020-12-21
    graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad,
                   res='M')  # 2019-12-30 -- 2020-12-21
    performance_matrix(x_d, y_target_l, d_real_l, tot_l, t_diff, y_voorraad_act)
    prognose_graph(x_d, y_prog_l, d_real_l, y_target_l)
    info_table(tot_l, d_real_l, HP, y_target_l, x_d, HC_HPend_l, Schouw_BIS, HPend_l, n_err)

    out0 = 'HPend afgesproken: ' + jaaroverzicht_data('target')
    out1 = 'HPend gerealiseerd: ' + jaaroverzicht_data('real')
    out2 = 'HPend gepland vanaf nu: ' + jaaroverzicht_data('plan')
    out3 = 'HPend voorspeld vanaf nu: ' + jaaroverzicht_data('prog')
    out4 = jaaroverzicht_data('HC_HPend')
    out5 = figure_data('graph_targets_M')
    out6 = figure_data('graph_targets_W')
    out7 = figure_data('project_performance')
    out8 = jaaroverzicht_data("HAS_werkvoorraad")

    return [out0, out1, out2, out3, out4, out5, out6, out7, out8]
