# from analyse_dashboard.analyse.functions import graph_overview, update_y_prog_l, targets
# from analyse_dashboard.analyse.functions import performance_matrix, prognose_graph
# from analyse_dashboard.analyse.functions import info_table, overview

from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from google.cloud import firestore
from layout.components.indicator import indicator

import pandas as pd
# import numpy as np

from app import app

# update value dropdown given selection in scatter chart
from data.graph import pie_chart, clickbar_lb, clickbar_hb
from data import collection
# from data.graph import info_table as graph_info_table


@app.callback(
    [Output("project-dropdown", 'value'),
     ],
    [Input("project_performance", 'clickData'),
     ]
)
def update_dropdown(value):
    return [value['points'][0]['text']]


# update graphs
@app.callback(
    [
        Output("graph_targets_W_container", 'hidden'),
        Output("graph_targets_M_container", 'hidden'),
        Output("info_globaal_container0", 'hidden'),
        Output("info_globaal_container1", 'hidden'),
        Output("info_globaal_container2", 'hidden'),
        Output("info_globaal_container3", 'hidden'),
        Output("info_globaal_container4", 'hidden'),
        Output("info_globaal_container5", 'hidden'),
        Output("info_globaal_container6", 'hidden'),
        Output("graph_speed_c", 'hidden'),
        Output("ww_c", 'hidden'),
        Output('FTU_table_c', 'hidden'),
        Output("graph_prog_c", "hidden"),
        Output("table_info", "hidden"),
        Output("Bar_LB_c", "hidden"),
        Output("Bar_HB_c", "hidden"),
        Output("pie_chart_overview_kpn_container", "hidden"),
        Output("Pie_NA_cid", "hidden"),
        Output("geo_plot", 'figure'),
        Output("table_c", 'children'),
        Output("geo_plot_c", "hidden"),
        Output("table_c", "hidden"),
    ],
    [
        Input("overzicht_button", 'n_clicks'),
        # Input("detail_button", "n_clicks")
    ],
    [
        State('project-dropdown', 'value'),
        State("aggregate_data", 'data'),
    ],
)
# def update_graphs(n_o, n_d, drop_selectie, mask_all):
def update_graphs(n_o, drop_selectie, mask_all):
    if drop_selectie is None:
        raise PreventUpdate
    if n_o == -1:
        hidden = True
    else:
        hidden = False
        # n_d = 0
    # if n_d in [1, 3, 5]:
    #     hidden1 = False
    #     fig = geomap_data_table(drop_selectie, mask_all)
    # else:
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
        hidden,  # info_globaal_container6
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

    fig_prog = collection.get_graph(client="kpn", graph_name="prognose_graph_dict", project=drop_selectie)
    for i, item in enumerate(fig_prog['data']):
        fig_prog['data'][i]['x'] = pd.to_datetime(item['x'])

    indicators = collection.get_document(collection="Data",
                                         graph_name="project_indicators",
                                         project=drop_selectie,
                                         client='kpn')
    indicator_types = ['weektarget', 'weekrealisatie', 'weekdelta', 'weekHCHPend', 'weeknerr']
    table_info = [indicator(value=indicators[el]['counts'],
                            previous_value=indicators[el]['counts_prev'],
                            title=indicators[el]['title'],
                            sub_title=indicators[el]['subtitle'],
                            font_color=indicators[el]['font_color']) for el in indicator_types]

    return [fig_prog, table_info, -1]


# update click bar charts
@app.callback(
    [
        Output("Bar_LB", "figure"),
        Output("Bar_HB", "figure"),
        Output("Pie_NA_c", "figure"),
        Output("aggregate_data", 'data'),
        Output("aggregate_data2", 'data'),
        # Output("detail_button", "n_clicks")
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

        doc = collection.get_document(collection="Data", client="kpn", graph_name="bar_names")['bar_names']
        if mask_all not in doc:
            mask_all = '0'
    else:
        mask_all = '0'
    barLB = clickbar_lb(drop_selectie, mask_all)
    barHB = clickbar_hb(drop_selectie, mask_all)
    pieNA = pie_chart(client='kpn', key=drop_selectie)

    # return [barLB, barHB, pieNA, mask_all, drop_selectie, 0]
    return [barLB, barHB, pieNA, mask_all, drop_selectie]


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
        # Output('info_globaal_container0_text', 'children'),
        # Output('info_globaal_container1_text', 'children'),
        # Output('info_globaal_container2_text', 'children'),
        # Output('info_globaal_container3_text', 'children'),
        # Output('info_globaal_container4_text', 'children'),
        # Output('info_globaal_container5_text', 'children'),
        # Output('graph_targets_M', 'figure'),
        # Output('graph_targets_W', 'figure'),
        Output('project_performance', 'figure'),
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
