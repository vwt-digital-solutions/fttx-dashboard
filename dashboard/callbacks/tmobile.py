import dash

from app import app
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from data import collection
from layout.components.graphs import pie_chart
import dash_bootstrap_components as dbc
from layout.components.figure import figure
from layout.components.indicator import indicator
from config import colors_vwt as colors

from google.cloud import firestore

client = "tmobile"


@app.callback(
    [
        Output("modal-sm", "is_open"),
        Output(f"indicator-modal-{client}", 'figure')
    ],
    [
        Input(f"indicator-late-{client}", "n_clicks"),
        Input(f"indicator-limited_time-{client}", "n_clicks"),
        Input(f"indicator-on_time-{client}", "n_clicks"),
        Input("close-sm", "n_clicks"),
    ],
    [
        State("modal-sm", "is_open"),
        State(f"indicator-data-{client}", "data")
    ]
)
def indicator_modal(late_clicks, limited_time_clicks, on_time_clicks, close_clicks, is_open, result):
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    print(changed_id)
    if "indicator" in changed_id and (late_clicks or limited_time_clicks or on_time_clicks):
        key = changed_id.partition("-")[-1].partition("-")[0]
        print(key)
        figure = pie_chart.get_html(labels=list(result[key]['cluster_redenna'].keys()),
                                    values=list(result[key]['cluster_redenna'].values()),
                                    title="Reden na",
                                    colors=[
                                        colors['green'],
                                        colors['yellow'],
                                        colors['red'],
                                        colors['vwt_blue'],
                                    ])

        return [not is_open, figure]

    if close_clicks:
        return [not is_open, {'data': None, 'layout': None}]
    return [is_open, {'data': None, 'layout': None}]


@app.callback(
    [
        Output(f"indicators-{client}", "children"),
        Output(f"indicator-data-{client}", 'data')
    ],
    [
        Input(f'project-dropdown-{client}', 'value')
    ]
)
def update_indicators(dropdown_selection):
    if dropdown_selection is None:
        raise PreventUpdate

    indicator_types = ['on_time', 'limited_time', 'late', 'ratio', 'ready_for_has']
    indicators = collection.get_document(collection="Data",
                                         graph_name="project_indicators",
                                         project=dropdown_selection,
                                         client=client)
    indicator_info = [indicator(value=indicators[el]['counts'],
                                previous_value=indicators[el].get('counts_prev', None),
                                title=indicators[el]['title'],
                                sub_title=indicators[el].get('subtitle', " "),
                                font_color=indicators[el].get('font_color', 'black'),
                                invert_delta=indicators[el].get("invert_delta", False),
                                id=f"indicator-{el}-{client}") for el in indicator_types]
    indicator_info = indicator_info + [
        dbc.Modal(
            [
                dbc.ModalBody(
                    figure(graph_id=f"indicator-modal-{client}",
                           className="",
                           figure={'data': None, 'layout': None})
                ),
                dbc.ModalFooter(
                    dbc.Button("Close", id="close-sm", className="ml-auto")
                ),
            ],
            id="modal-sm",
            size="lg",
            centered=True,
        )
    ]

    return [indicator_info, indicators]


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
        # Output('info_globaal_container0_text', 'children'),
        # Output('info_globaal_container1_text', 'children'),
        # Output('info_globaal_container2_text', 'children'),
        # Output('info_globaal_container3_text', 'children'),
        # Output('info_globaal_container4_text', 'children'),
        # Output('info_globaal_container5_text', 'children'),
        # Output('graph_targets_M', 'figure'),
        # Output('graph_targets_W', 'figure'),
        Output(f'project-performance-{client}', 'figure'),
    ],
    [
        Input(f'table_FTU_{client}', 'data'),
    ],
)
def FTU_update(data):

    record = dict(graph_name='project_dates', client=client)
    FTU0 = {}
    FTU1 = {}
    for el in data:
        FTU0[el['Project']] = el['Eerste HAS aansluiting (FTU0)']
        FTU1[el['Project']] = el['Laatste HAS aansluiting (FTU1)']
    record['record'] = {}
    record['record']['FTU0'] = FTU0
    record['record']['FTU1'] = FTU1
    print(record)
    firestore.Client().collection('Data').document(f'{client}_project_dates').set(record)

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
    out8 = collection.get_graph(client=client, graph_name='project_performance')

    # return [out0, out1, out2, out3, out4, out5, out6, out7, out8]
    return [out8]
