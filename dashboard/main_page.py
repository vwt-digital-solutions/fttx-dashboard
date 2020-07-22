import pandas as pd
import numpy as np
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import dash_html_components as html
import dash_table
import api
import json
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from elements import table_styles
from google.cloud import firestore
from app import app
from analyse_dashboard.analyse.functions import graph_overview, update_y_prog_l, targets
from analyse_dashboard.analyse.functions import performance_matrix, prognose_graph
from analyse_dashboard.analyse.functions import info_table, overview, from_rd

layout = dict(
    autosize=True,
    automargin=True,
    margin=dict(le=30, r=30, b=20, t=40),
    hovermode="closest",
    plot_bgcolor="#F9F9F9",
    paper_bgcolor="#F9F9F9",
    legend=dict(font=dict(size=10), orientation="h"),
)


# APP LAYOUT
def get_body():
    page = html.Div(
        [
            dcc.Store(id="aggregate_data",
                      data=None),
            dcc.Store(id="aggregate_data2",
                      data=None),
            dcc.Store(id="aggregate_data3",
                      data=None),
            html.Div(
                [
                    html.Div(
                        [
                            html.Img(
                                src=app.get_asset_url("ODH_logo_original.png"),
                                id="DAT-logo",
                                style={
                                    "height": "70px",
                                    "width": "auto",
                                    "margin-bottom": "15px",
                                    "margin-left": "115px"
                                },
                            ),
                        ],
                        className="one-third column",
                        style={'textAlign': 'left'}
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H3(
                                        "Status projecten KPN in 2020",
                                        style={"margin-bottom": "0px", "margin-left": "75px"},
                                    ),
                                    html.P(id='date_update',
                                           children='Laatste data update: ' + generate_graphs(85, None, None),
                                           style={"margin-bottom": "0px", "margin-left": "75px"},
                                           )
                                ],
                            )
                        ],
                        className="one-third column",
                        id="title",
                        style={'textAlign': 'center'}
                    ),
                    html.Div(
                        [
                            html.Img(
                                src=app.get_asset_url("vqd.png"),
                                id="vqd-image",
                                style={
                                    "height": "100px",
                                    "width": "auto",
                                    "margin-bottom": "15px",
                                    "margin-right": "0px"
                                },
                            )
                        ],
                        className="one-third column",
                        style={'textAlign': 'right'}
                    ),
                ],
                id="header",
                className="row",
                style={"margin-bottom": "25px"},
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.H6(id="info_globaal_0"),
                            html.P([html.Strong('Outlook (KPN)')]),
                            html.P(id="info_globaal_00", children='HPend afgesproken: ' + generate_graphs(80, None, None))
                        ],
                        id="info_globaal_container0",
                        className="pretty_container column",
                        hidden=False,
                    ),
                    html.Div(
                        [
                            html.H6(id="info_globaal_1"),
                            html.P([html.Strong('Realisatie (FC)')]),
                            html.P(id="info_globaal_01", children='HPend gerealiseerd: ' + generate_graphs(81, None, None))
                        ],
                        id="info_globaal_container1",
                        className="pretty_container column",
                        hidden=False,
                    ),
                    html.Div(
                        [
                            html.H6(id="info_globaal_2"),
                            html.P([html.Strong('Planning (VWT)')]),
                            html.P(id="info_globaal_02", children='HPend gepland vanaf nu: ' + generate_graphs(82, None, None))
                        ],
                        id="info_globaal_container2",
                        className="pretty_container column",
                        hidden=False,
                    ),
                    html.Div(
                        [
                            html.H6(id="info_globaal_3"),
                            html.P([html.Strong('Voorspelling (VQD)')]),
                            html.P(id="info_globaal_03", children='HPend voorspeld vanaf nu: ' + generate_graphs(83, None, None)['prog'])
                        ],
                        id="info_globaal_container3",
                        className=generate_graphs(83, None, None)['prog_c'] + ' column',
                        hidden=False,
                    ),
                    html.Div(
                        [
                            html.H6(id="info_globaal_4"),
                            html.P([html.Strong('Actuele HC / HPend')]),
                            html.P(id="info_globaal_04", children=generate_graphs(84, None, None))
                        ],
                        id="info_globaal_container4",
                        className="pretty_container column",
                        hidden=False,
                    ),
                ],
                id="info-container1",
                className="container-display",
            ),
            html.Div(
                [
                    html.Div(
                            [dcc.Graph(id='graph_targets_ov', figure=generate_graphs(42, None, None))],
                            id='graph_targets_overall_c',
                            className="pretty_container column",
                            hidden=False,
                    ),
                    html.Div(
                            [dcc.Graph(id='graph_targets_m', figure=generate_graphs(41, None, None))],
                            id='graph_targets_overallM_c',
                            className="pretty_container column",
                            hidden=False,
                    ),
                ],
                id="main_graphs0",
                className="container-display",
            ),
            html.Div(
                [
                    html.Div(
                            [dcc.Graph(figure=generate_graphs(2, None, None),
                                       id='project_performance')],
                            id='graph_speed_c',
                            className="pretty_container column",
                            hidden=False,
                    ),
                    html.Div([
                                html.Div(id='ww_c',
                                         children=dcc.Input(id='ww', value=' ', type='text'),
                                         className="pretty_container column",
                                         hidden=False,
                                         ),
                                html.Div(
                                        generate_graphs(9, None, None),
                                        id='FTU_table_c',
                                        className="pretty_container column",
                                        hidden=False,
                                ),
                    ],
                               className="pretty_container column",
                    ),
                ],
                className="container-display",
            ),
            html.Div(
                [
                    html.Div(
                            [dcc.Dropdown(id='project-dropdown',
                                          options=generate_graphs(3, None, None),
                                          value=None)],
                            className="two-third column",
                    ),
                    html.Div(
                            [dbc.Button('Terug naar overzicht alle projecten', id='overzicht_button')],
                            className="one-third column",
                    ),
                ],
                className="container-display",
                id="title",
            ),
            html.Div(
                [
                    html.Div(
                            [dcc.Graph(id="graph_prog")],
                            id='graph_prog_c',
                            className="pretty_container column",
                            hidden=True,
                    ),
                    # html.Div(
                    #         [dcc.Graph(id="graph_targets")],
                    #         id='graph_targets_c',
                    #         className="pretty_container column",
                    #         hidden=True,
                    # ),
                    html.Div(
                            [dcc.Graph(id="Bar_LB")],
                            id='Bar_LB_c',
                            className="pretty_container column",
                            hidden=True,
                    ),
                    html.Div(
                            [dcc.Graph(id="Bar_HB")],
                            id='Bar_HB_c',
                            className="pretty_container column",
                            hidden=True,
                    ),
                ],
                id="main_graphs",
                className="container-display",
            ),
            html.Div(
                [
                    html.Div(
                            id='table_info',
                            className="pretty_container column",
                            hidden=True,
                    ),

                ],
                id="main_graphs",
                className="container-display",
            ),
            html.Div(
                    [dbc.Button('Project details [eerste 3000 resultaten]', id='detail_button')],
                    className="one-third column"
            ),
            html.Div(
                [
                    html.Div(
                            [dcc.Graph(id="geo_plot")],
                            id='geo_plot_c',
                            className="pretty_container column",
                            hidden=True,
                            ),
                ],
                id="details",
                className="container-display",
            ),
            html.Div(
                [
                    html.Div(
                        id='table_c',
                        className="pretty_container",
                        hidden=True,
                    ),
                ],
                id="details",
                className="container-display",
            ),
        ],
        id="mainContainer",
        style={"display": "flex", "flex-direction": "column"},
    )
    return page


# update value dropdown given selection in scatter chart
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
     Output("graph_targets_overall_c", 'hidden'),
     Output("graph_targets_overallM_c", 'hidden'),
     Output("info_globaal_container0", 'hidden'),
     Output("info_globaal_container1", 'hidden'),
     Output("info_globaal_container2", 'hidden'),
     Output("info_globaal_container3", 'hidden'),
     Output("info_globaal_container4", 'hidden'),
     Output("graph_speed_c", 'hidden'),
     Output("ww_c", 'hidden'),
     Output('FTU_table_c', 'hidden'),
     Output("graph_prog_c", "hidden"),
     Output("graph_targets_c", "hidden"),
     Output("table_info", "hidden"),
     Output("Bar_LB_c", "hidden"),
     Output("Bar_HB_c", "hidden"),
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
        fig = generate_graphs(7, drop_selectie, mask_all)
    else:
        hidden1 = True
        fig = dict(geo={'data': None, 'layout': dict()}, table=None)

    return [hidden, hidden, hidden, hidden, hidden, hidden, hidden, hidden, hidden, hidden,
            not hidden, not hidden, not hidden, not hidden, not hidden,
            fig['geo'], fig['table'], hidden1, hidden1]


# update middle-top charts given dropdown selection
@app.callback(
    [
     Output("graph_prog", 'figure'),
     Output("graph_targets", 'figure'),
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

    fig_bish = generate_graphs(0, drop_selectie, None)
    fig_prog = generate_graphs(1, drop_selectie, None)
    table_info = generate_graphs(8, drop_selectie, None)

    return [fig_prog, fig_bish, table_info, -1]


# update click bar charts
@app.callback(
    [
     Output("Bar_LB", "figure"),
     Output("Bar_HB", "figure"),
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

    barLB = generate_graphs(5, drop_selectie, mask_all)
    barHB = generate_graphs(6, drop_selectie, mask_all)

    return [barLB, barHB, mask_all, drop_selectie, 0]


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
     Output('info_globaal_00', 'children'),
     Output('info_globaal_01', 'children'),
     Output('info_globaal_02', 'children'),
     Output('info_globaal_03', 'children'),
     Output('info_globaal_04', 'children'),
     Output('graph_targets_ov', 'figure'),
     Output('graph_targets_m', 'figure'),
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
    firestore.Client().collection('Graphs').document(record['id']).set(record)

    # to update overview graphs:
    HC_HPend = next(firestore.Client().collection('Graphs').where('id', '==', 'jaaroverzicht').get()).to_dict()['HC_HPend']
    doc = next(firestore.Client().collection('Graphs').where('id', '==', 'analysis2').get()).to_dict()
    doc2 = next(firestore.Client().collection('Graphs').where('id', '==', 'analysis3').get()).to_dict()
    x_d = pd.to_datetime(doc['x_d'])
    tot_l = doc['tot_l']
    HP = doc['HP']
    HC_HPend_l = doc['HC_HPend_l']
    Schouw_BIS = doc['Schouw_BIS']
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
    graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, res='W-MON')  # 2019-12-30 -- 2020-12-21
    graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, res='M')  # 2019-12-30 -- 2020-12-21
    performance_matrix(x_d, y_target_l, d_real_l, tot_l, t_diff, y_voorraad_act)
    prognose_graph(x_d, y_prog_l, d_real_l, y_target_l)
    info_table(tot_l, d_real_l, HP, y_target_l, x_d, HC_HPend_l, Schouw_BIS, HPend_l)

    out0 = 'HPend afgesproken: ' + generate_graphs(80, None, None)
    out1 = 'HPend gerealiseerd: ' + generate_graphs(81, None, None)
    out2 = 'HPend gepland vanaf nu: ' + generate_graphs(82, None, None)
    out3 = 'HPend voorspeld vanaf nu: ' + generate_graphs(83, None, None)['prog']
    out4 = generate_graphs(84, None, None)
    out5 = generate_graphs(42, None, None)
    out6 = generate_graphs(41, None, None)
    out7 = generate_graphs(2, None, None)

    return [out0, out1, out2, out3, out4, out5, out6, out7]


# HELPER FUNCTIES
def generate_graphs(flag, drop_selectie, mask_all):

    if flag == 80:
        fig = api.get('/Graphs?id=jaaroverzicht')[0]['target']

    if flag == 81:
        fig = api.get('/Graphs?id=jaaroverzicht')[0]['real']

    if flag == 82:
        fig = api.get('/Graphs?id=jaaroverzicht')[0]['plan']

    if flag == 83:
        fig = api.get('/Graphs?id=jaaroverzicht')[0]

    if flag == 84:
        fig = api.get('/Graphs?id=jaaroverzicht')[0]['HC_HPend']

    if flag == 85:
        fig = api.get('/Graphs?id=update_date')[0]['date']

    # BIS/HAS
    if flag == 0:
        fig = api.get('/Graphs?id=' + drop_selectie)[0]['figure']

    # prognose
    if flag == 1:
        fig = api.get('/Graphs?id=' + 'project_' + drop_selectie)[0]['figure']
        for i, item in enumerate(fig['data']):
            fig['data'][i]['x'] = pd.to_datetime(item['x'])

    # project speed
    if flag == 2:
        fig = api.get('/Graphs?id=project_performance')[0]['figure']

    # labels
    if flag == 3:
        fig = api.get('/Graphs?id=pnames')[0]['filters']

    # targets
    if flag == 41:
        fig = api.get('/Graphs?id=graph_targets_W')[0]['figure']

    if flag == 42:
        fig = api.get('/Graphs?id=graph_targets_M')[0]['figure']

    # clickbar LB
    if flag == 5:
        fig = api.get('/Graphs?id=' + drop_selectie + '_bar_filters_' + mask_all)[0]['bar']
        bar = {}
        for key in fig:
            if 'LB' in key:
                bar[key] = [int(fig[key])]
        labels = ['Schouwen', 'BIS', 'Montage-lasAP', 'Montage-lasDP', 'HAS']
        barLB1HC = dict(x=labels,
                        y=bar['SchouwenLB1'] + bar['BISLB1'] + bar['Montage-lasAPLB1'] + bar['Montage-lasDPLB1'] + bar['HASLB1'],
                        name='Opgeleverd HC',
                        type='bar',
                        marker=dict(color='rgb(0, 200, 0)'),
                        )
        barLB1HP = dict(x=labels,
                        y=[0] + [0] + [0] + [0] + bar['HASLB1HP'],
                        name='Opgeleverd zonder HC',
                        type='bar',
                        marker=dict(color='rgb(200, 200, 0)')
                        )
        barLB0 = dict(x=labels,
                      y=bar['SchouwenLB0'] + bar['BISLB0'] + bar['Montage-lasAPLB0'] + bar['Montage-lasDPLB0'] + bar['HASLB0'],
                      name='Niet opgeleverd',
                      type='bar',
                      marker=dict(color='rgb(200, 0, 0)')
                      )
        fig = dict(data=[barLB1HC, barLB1HP, barLB0],
                   layout=dict(barmode='stack',
                               clickmode='event+select',
                               showlegend=True,
                               height=350,
                               title={'text': 'Status oplevering per fase (LB)<br>[selectie resets na 3x klikken]:', 'x': 0.5},
                               yaxis={'title': '[aantal woningen]'},
                               ))

    # clickbar HB
    if flag == 6:
        fig = api.get('/Graphs?id=' + drop_selectie + '_bar_filters_' + mask_all)[0]['bar']
        bar = {}
        for key in fig:
            if 'HB' in key:
                bar[key] = [int(fig[key])]
        labels = ['Schouwen', 'BIS', 'Montage-lasAP', 'Montage-lasDP', 'HAS']
        barHB1HC = dict(x=labels,
                        y=bar['SchouwenHB1'] + bar['BISHB1'] + bar['Montage-lasAPHB1'] + bar['Montage-lasDPHB1'] + bar['HASHB1'],
                        name='Opgeleverd HC',
                        type='bar',
                        marker=dict(color='rgb(0, 200, 0)')
                        )
        barHB1HP = dict(x=labels,
                        y=[0] + [0] + [0] + [0] + bar['HASHB1HP'],
                        name='Opgeleverd zonder HC',
                        type='bar',
                        marker=dict(color='rgb(200, 200, 0)')
                        )
        barHB0 = dict(x=labels,
                      y=bar['SchouwenHB0'] + bar['BISHB0'] + bar['Montage-lasAPHB0'] + bar['Montage-lasDPHB0'] + bar['HASHB0'],
                      name='Niet opgeleverd',
                      type='bar',
                      marker=dict(color='rgb(200, 0, 0)')
                      )
        fig = dict(data=[barHB1HC, barHB1HP, barHB0],
                   layout=dict(barmode='stack',
                               clickmode='event+select',
                               showlegend=True,
                               height=350,
                               title={'text': 'Status oplevering per fase (HB & Duplex)<br>[selectie resets na 3x klikken]:', 'x': 0.5},
                               yaxis={'title': '[aantal woningen]'},
                               ))

    # geomap & data table
    if flag == 7:
        if mask_all == '0':
            records = api.get('/Projects?project=' + drop_selectie)
            df = pd.DataFrame(records)
        else:
            mask = json.loads(api.get('/Graphs?id=' + drop_selectie + '_bar_filters_' + mask_all)[0]['mask'])
            dataframe = []
            for m in mask:
                dataframe += api.get('/Projects?sleutel=' + m)
            df = pd.DataFrame(dataframe)

        if not df[~df['x_locatie_rol'].isna()].empty:

            df['clr'] = 50
            df.loc[df['opleverdatum'].isna(), ('clr')] = 0
            df['clr-DP'] = 0
            df.loc[df['opleverstatus'] != 0, ('clr-DP')] = 50  # 25 == geel
            df['x_locatie_rol'] = df['x_locatie_rol'].str.replace(',', '.').astype(float)
            df['y_locatie_rol'] = df['y_locatie_rol'].str.replace(',', '.').astype(float)
            df['x_locatie_dp'] = df['x_locatie_dp'].str.replace(',', '.').astype(float)
            df['y_locatie_dp'] = df['y_locatie_dp'].str.replace(',', '.').astype(float)
            df['Lat'], df['Long'] = from_rd(df['x_locatie_rol'], df['y_locatie_rol'])
            df['Lat_DP'], df['Long_DP'] = from_rd(df['x_locatie_dp'], df['y_locatie_dp'])
            df['Size'] = 7
            df['Size_DP'] = 14

            # this is a default public token obtained from a free account on https://account.mapbox.com/
            # and can there be refreshed at any moment
            mapbox_at = api.get('/Graphs?id=token_mapbox')[0]['token']
            normalized_size = df['Size_DP'].to_list() + df['Size'].to_list()
            map_data = [
                go.Scattermapbox(
                    lat=df['Lat_DP'].to_list() + df['Lat'].to_list(),
                    lon=df['Long_DP'].to_list() + df['Long'].to_list(),
                    mode='markers',
                    marker=dict(
                        cmax=50,
                        cmin=0,
                        color=df['clr-DP'].to_list() + df['clr'].to_list(),
                        colorscale=['green', 'yellow', 'red'],
                        reversescale=True,
                        size=normalized_size * 7,
                    ),
                    text=df['clr'],
                    hoverinfo='text'
                )
            ]
            map_layout = dict(
                autosize=True,
                automargin=True,
                margin={'l': 30, 'r': 30, 'b': 30, 't': 120},
                height=500,
                hovermode="closest",
                plot_bgcolor="#F9F9F9",
                paper_bgcolor="#F9F9F9",
                legend=dict(font=dict(size=10), orientation="h"),
                title="Status oplevering per woning (kleine marker) & DP (grote marker)<br>[groen = opgeleverd, rood = niet opgeleverd]",
                mapbox=dict(
                    accesstoken=mapbox_at,
                    style="light",
                    center=dict(lon=df['Long'].mean(), lat=df['Lat'].mean()),
                    zoom=13,
                ),
            )

            fig = dict(geo={'data': map_data, 'layout': map_layout})
        else:
            fig = dict(geo={'data': None, 'layout': dict()})

        df['uitleg redenna'] = df['redenna'].map(api.get('/Graphs?id=reden_mapping')[0]['map'])
        df = df[['sleutel', 'opleverdatum', 'hasdatum', 'opleverstatus', 'uitleg redenna']].sort_values(by='hasdatum')
        df_table = dash_table.DataTable(
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict("rows"),
            filter_action="native",
            sort_action="native",
            style_table={'overflowX': 'auto'},
            style_header=table_styles['header'],
            style_cell=table_styles['cell']['action'],
            style_filter=table_styles['filter'],
            css=[{
                'selector': 'table',
                'rule': 'width: 100%;'
            }],
        )
        fig['table'] = df_table

    if flag == 8:
        df = pd.read_json(api.get('/Graphs?id=info_table')[0]['table'], orient='records')
        df = df[api.get('/Graphs?id=info_table')[0]['col']]
        fig = dash_table.DataTable(
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict("rows"),
            filter_action="native",
            sort_action="native",
            style_table={'overflowX': 'auto'},
            style_header=table_styles['header'],
            style_cell=table_styles['cell']['action'],
            style_filter=table_styles['filter'],
            css=[{
                'selector': 'table',
                'rule': 'width: 100%;'
            }],
        )

    if flag == 9:
        df = pd.DataFrame(columns=['Project', 'FTU0', 'FTU1'])
        df['Project'] = list(api.get('/Graphs?id=analysis')[0]['FTU0'].keys())
        df['FTU0'] = list(api.get('/Graphs?id=analysis')[0]['FTU0'].values())
        df['FTU1'] = list(api.get('/Graphs?id=analysis')[0]['FTU1'].values())
        fig = dash_table.DataTable(
            id='table_FTU',
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict("rows"),
            filter_action="native",
            sort_action="native",
            style_table={'overflowX': 'auto'},
            style_header=table_styles['header'],
            style_cell=table_styles['cell']['action'],
            style_filter=table_styles['filter'],
            css=[{
                'selector': 'table',
                'rule': 'width: 100%;'
            }],
            editable=False,
        )

    return fig
