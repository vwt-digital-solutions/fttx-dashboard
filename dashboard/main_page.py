import pandas as pd
import numpy as np
import datetime
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
                                        style={"margin-bottom": "25px",
                                               "margin-left": "75px",
                                               },
                                    ),
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
                                        children=generate_graphs(9, None, None),
                                        id='FTU_table_c',
                                        className="pretty_container column",
                                        hidden=False,
                                ),
                    ]),
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

    Output("uitleg_collapse", "hidden"),


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
                dataframe += api.get('/Projects?id=' + drop_selectie + '_' + m)
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
            mapbox_at = 'pk.eyJ1IjoiYXZhbnR1cm5ob3V0IiwiYSI6ImNrOGl4Y2o3ZTA5MjMzbW53a3dicTRnMnIifQ.FdFexMQbqQrZBNMEZkYvvg'
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
        df = pd.read_json(api.get('/Graphs?id=info_table')[0]['table'], orient='records').sort_values(by=['real vs KPN'], ascending=True)
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


def from_rd(x: int, y: int) -> tuple:
    x0 = 155000
    y0 = 463000
    phi0 = 52.15517440
    lam0 = 5.38720621

    # Coefficients or the conversion from RD to WGS84
    Kp = [0, 2, 0, 2, 0, 2, 1, 4, 2, 4, 1]
    Kq = [1, 0, 2, 1, 3, 2, 0, 0, 3, 1, 1]
    Kpq = [3235.65389, -32.58297, -0.24750, -0.84978, -0.06550, -0.01709,
           -0.00738, 0.00530, -0.00039, 0.00033, -0.00012]

    Lp = [1, 1, 1, 3, 1, 3, 0, 3, 1, 0, 2, 5]
    Lq = [0, 1, 2, 0, 3, 1, 1, 2, 4, 2, 0, 0]
    Lpq = [5260.52916, 105.94684, 2.45656, -0.81885, 0.05594, -0.05607,
           0.01199, -0.00256, 0.00128, 0.00022, -0.00022, 0.00026]

    """
    Converts RD coordinates into WGS84 coordinates
    """
    dx = 1E-5 * (x - x0)
    dy = 1E-5 * (y - y0)
    latitude = phi0 + sum([v * dx ** Kp[i] * dy ** Kq[i]
                           for i, v in enumerate(Kpq)]) / 3600
    longitude = lam0 + sum([v * dx ** Lp[i] * dy ** Lq[i]
                            for i, v in enumerate(Lpq)]) / 3600
    return latitude, longitude


def graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, res):
    if 'W' in res:
        n_now = int((pd.Timestamp.now() - pd.to_datetime('2019-12-30')).days / 7) + 1
        n_d = int((pd.Timestamp.now() - pd.to_datetime('2020-' + str(datetime.date.today().month) + '-01')).days / 7)
        x_ticks = list(range(n_now - n_d, n_now + 5 - n_d))
        x_ticks_text = [datetime.datetime.strptime('2020-W' + str(int(el-1)) + '-1', "%Y-W%W-%w").date().strftime(
            '%Y-%m-%d') + '<br>W' + str(el) for el in x_ticks]
        x_range = [n_now - n_d - 0.5, n_now + 4.5 - n_d]
        y_range = [0, 3000]
        width = 0.08
        text_title = 'Maandoverzicht'
        period = ['2019-12-23', '2020-12-27']
        close = 'left'
        loff = '-1W-MON'
        x = df_prog[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum().index.week.to_list()
        x[0] = 0
    if 'M' == res:
        n_now = datetime.date.today().month
        x_ticks = list(range(0, 13))
        x_ticks_text = ['dec', 'jan', 'feb', 'maa', 'apr', 'mei', 'jun', 'jul', 'aug', 'sep', 'okt', 'nov', 'dec']
        x_range = [0.5, 12.5]
        y_range = [0, 18000]
        width = 0.2
        text_title = 'Jaaroverzicht'
        period = ['2019-12-23', '2020-12-27']
        close = 'left'
        loff = None
        x = df_prog[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum().index.month.to_list()
        x[0] = 0

    prog = df_prog[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d'].to_list()
    target = df_target[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d'].to_list()
    real = df_real[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d'].to_list()
    plan = df_plan[period[0]:period[1]].resample(res, closed=close, loffset=loff).sum()['d'].to_list()
    plan[0:n_now] = real[0:n_now]  # gelijk trekken afgelopen periode

    if 'M' == res:
        jaaroverzicht = dict(id='jaaroverzicht', target=str(round(sum(target[1:]))), real=str(round(sum(real[1:]))),
                             plan=str(round(sum(plan[n_now:]) - real[n_now])), prog=str(round(sum(prog[n_now:]) - real[n_now])),
                             HC_HPend=str(HC_HPend), prog_c='pretty_container')
        if jaaroverzicht['prog'] < jaaroverzicht['plan']:
            jaaroverzicht['prog_c'] = 'pretty_container_red'

    bar_now = dict(x=[n_now],
                   y=[y_range[1]],
                   name='Huidige week',
                   type='bar',
                   marker=dict(color='rgb(0, 0, 0)'),
                   width=0.5*width,
                   )
    bar_t = dict(x=[el - 0.5*width for el in x],
                 y=target,
                 name='Outlook (KPN)',
                 type='bar',
                 marker=dict(color='rgb(170, 170, 170)'),
                 width=width,
                 )
    bar_pr = dict(x=x,
                  y=prog,
                  name='Voorspelling (VQD)',
                  mode='markers',
                  marker=dict(color='rgb(200, 200, 0)', symbol='diamond', size=15),
                  #   width=0.2,
                  )
    bar_r = dict(x=[el + 0.5*width for el in x],
                 y=real,
                 name='Realisatie (FC)',
                 type='bar',
                 marker=dict(color='rgb(0, 200, 0)'),
                 width=width,
                 )
    bar_pl = dict(x=x,
                  y=plan,
                  name='Planning HP (VWT)',
                  type='lines',
                  marker=dict(color='rgb(200, 0, 0)'),
                  width=width,
                  )
    fig = {
           'data': [bar_pr, bar_pl, bar_r, bar_t, bar_now],
           'layout': {
                      'barmode': 'stack',
                      #   'clickmode': 'event+select',
                      'showlegend': True,
                      'legend': {'orientation': 'h', 'x': -0.075, 'xanchor': 'left', 'y': -0.25, 'font': {'size': 10}},
                      'height': 300,
                      'margin': {'l': 5, 'r': 15, 'b': 5, 't': 40},
                      'title': {'text': text_title},
                      'xaxis': {'range': x_range,
                                'tickvals': x_ticks,
                                'ticktext': x_ticks_text,
                                'title': ' '},
                      'yaxis': {'range': y_range, 'title': 'Aantal HPend'},
                      #   'annotations': [dict(x=x_ann, y=y_ann, text=jaaroverzicht, xref="x", yref="y",
                      #                   ax=0, ay=0, alignment='left', font=dict(color="black", size=15))]
                      },
          }
    if 'W' in res:
        record = dict(id='graph_targets_W', figure=fig)
    if 'M' == res:
        firestore.Client().collection('Graphs').document('jaaroverzicht').set(jaaroverzicht)
        record = dict(id='graph_targets_M', figure=fig)
    firestore.Client().collection('Graphs').document(record['id']).set(record)


def overview(x_d, y_prog_l, tot_l, d_real_l, HP, y_target_l):

    df_prog = pd.DataFrame(index=x_d, columns=['d'], data=0)
    for key in y_prog_l:
        y_prog = y_prog_l[key] / 100 * tot_l[key]
        df_prog += pd.DataFrame(index=x_d, columns=['d'], data=y_prog).diff().fillna(0)

    df_target = pd.DataFrame(index=x_d, columns=['d'], data=0)
    for key in y_target_l:
        y_target = y_target_l[key] / 100 * tot_l[key]
        df_target += pd.DataFrame(index=x_d, columns=['d'], data=y_target).diff().fillna(0)

    df_real = pd.DataFrame(index=x_d, columns=['d'], data=0)
    for key in d_real_l:
        y_real = (d_real_l[key] / 100 * tot_l[key]).diff().fillna((d_real_l[key] / 100 * tot_l[key]).iloc[0])
        y_real = y_real.rename(columns={'Aantal': 'd'})
        y_real.index = x_d[y_real.index]
        df_real = df_real.add(y_real, fill_value=0)

    df_plan = pd.DataFrame(index=x_d, columns=['d'], data=0)
    y_plan = pd.DataFrame(index=pd.date_range(start='30-12-2019', periods=len(HP['HPendT']), freq='W-MON'),
                          columns=['d'], data=HP['HPendT'])
    y_plan = y_plan.cumsum().resample('D').mean().interpolate().diff().fillna(y_plan.iloc[0])
    df_plan = df_plan.add(y_plan, fill_value=0)

    # plot option
    # import matplotlib.pyplot as plt
    # test = df_real.resample('M', closed='left', loffset=None).sum()['d']
    # fig, ax = plt.subplots(figsize=(14,8))
    # ax.bar(x=test.index[0:15].strftime('%Y-%m'), height=test[0:15], width=0.5)
    # plt.savefig('Graphs/jaaroverzicht_2019_2020.png')

    return df_prog, df_target, df_real, df_plan


def update_y_prog_l(date_FTU0, d_real_l, t_shift, rc1, rc2, y_prog_l, x_d, x_prog, cutoff):
    rc1_mean = sum(rc1.values()) / len(rc1.values())
    rc2_mean = sum(rc2.values()) / len(rc2.values())
    for key in date_FTU0:
        if key not in d_real_l:  # the case of no realisation date
            t_shift[key] = x_prog[x_d == date_FTU0[key]][0]
            b1_mean = -(rc1_mean * (t_shift[key] + 14))  # to include delay of two week
            y_prog1 = b1_mean + rc1_mean * x_prog
            b2_mean = cutoff - (rc2_mean * x_prog[y_prog1 >= cutoff][0])
            y_prog2 = b2_mean + rc2_mean * x_prog
            y_prog_l[key] = y_prog1.copy()
            y_prog_l[key][y_prog1 >= cutoff] = y_prog2[y_prog1 >= cutoff]
            y_prog_l[key][y_prog_l[key] > 100] = 100
            y_prog_l[key][y_prog_l[key] < 0] = 0

    return y_prog_l, t_shift


def targets(x_prog, x_d, t_shift, date_FTU0, date_FTU1, rc1, d_real_l):
    # to add target info KPN in days uitgaande van FTU0 en FTU1
    y_target_l = {}
    t_diff = {}
    for key in t_shift:
        if (key in date_FTU0) & (key in date_FTU1):
            t_start = x_prog[x_d == date_FTU0[key]][0]
            t_max = x_prog[x_d == date_FTU1[key]][0]
            t_diff[key] = t_max - t_start - 14  # two weeks round up
            rc = 100 / t_diff[key]  # target naar KPN is 100% HPend
        if (key in date_FTU0) & (key not in date_FTU1):  # estimate target based on average projectspeed
            t_start = x_prog[x_d == date_FTU0[key]][0]
            t_diff[key] = (100 / (sum(rc1.values()) / len(rc1.values())) - 14)[0]  # two weeks round up
            rc = 100 / t_diff[key]  # target naar KPN is 100% HPend
        if (key not in date_FTU0):  # project has finished, estimate target on what has been done
            t_start = d_real_l[key].index.min()
            t_max = d_real_l[key].index.max()
            t_diff[key] = t_max - t_start - 14  # two weeks round up
            rc = 100 / t_diff[key]  # target naar KPN is 100% HPend

        b = -(rc * (t_start + 14))  # two weeks startup
        y_target = b + rc * x_prog
        y_target[y_target > 100] = 100
        y_target_l[key] = y_target

    for key in y_target_l:
        y_target_l[key][y_target_l[key] > 100] = 100
        y_target_l[key][y_target_l[key] < 0] = 0

    return y_target_l, t_diff


def performance_matrix(x_d, y_target_l, d_real_l, tot_l, t_diff, y_voorraad_act):
    n_now = int((pd.Timestamp.now() - x_d[0]).days)
    x = []
    y = []
    names = []
    for key in y_target_l:
        if key in d_real_l:
            x += [round((d_real_l[key].max() - y_target_l[key][n_now]))[0]]
        else:
            x += [0]
        y_voorraad = tot_l[key] / t_diff[key] * 7 * 9  # op basis van 9 weken voorraad
        if y_voorraad > 0:
            y += [round(y_voorraad_act[key] / y_voorraad * 100)]
        else:
            y += [0]
        names += [key]

    x_max = 30  # + max([abs(min(x)), abs(max(x))])
    x_min = - x_max
    y_min = - 30
    y_max = 250  # + max([abs(min(y)), abs(max(y))])
    y_voorraad_p = 90
    fig = {'data': [
                    {
                     'x': [x_min, 1 / 70 * x_min, 1 / 70 * x_min, x_min],
                     'y': [y_min, y_min, y_voorraad_p, y_voorraad_p],
                     'name': 'Trace 2',
                     'mode': 'lines',
                     'fill': 'toself',
                     'opacity': 1,
                     'line': {'color': 'rgb(200, 0, 0)'}
                     },
                    {
                     'x': [1 / 70 * x_min, 1 / 70 * x_max, 1 / 70 * x_max, 15, 15, 1 / 70 * x_min],
                     'y': [y_min, y_min, y_voorraad_p, y_voorraad_p, 150, 150],
                     'name': 'Trace 2',
                     'mode': 'lines',
                     'fill': 'toself',
                     'opacity': 1,
                     'line': {'color': 'rgb(0, 200, 0)'}
                     },
                    {
                     'x': [x_min, 1 / 70 * x_min, 1 / 70 * x_min, 15,  15, 1 / 70 * x_max,
                           1 / 70 * x_max,  x_max, x_max, x_min, x_min, 1 / 70 * x_min],
                     'y': [y_voorraad_p, y_voorraad_p, 150, 150, y_voorraad_p, y_voorraad_p,
                           y_min, y_min, y_max, y_max, y_voorraad_p, y_voorraad_p],
                     'name': 'Trace 2',
                     'mode': 'lines',
                     'fill': 'toself',
                     'opacity': 1,
                     'line': {'color': 'rgb(200, 200, 0)'}
                     },
                    {
                     'x':  x,
                     'y': y,
                     'text': names,
                     'name': 'Trace 1',
                     'mode': 'markers',
                     'marker': {'size': 15, 'color': 'rgb(0, 0, 0)'}
                     }],
           'layout': {'clickmode': 'event+select',
                      'xaxis': {'title': '(HPend gerealiseerd - Target KPN) /  HPend totaal [%]', 'range': [x_min, x_max],
                                'zeroline': False},
                      'yaxis': {'title': '(Geschouwd + BIS) / werkvoorraad [%]', 'range': [y_min, y_max], 'zeroline': False},
                      'showlegend': False,
                      'title': {'text': 'Krijg alle projecten in het groene vlak doormiddel van de pijlen te volgen'},
                      'annotations': [dict(x=-20, y=50, ax=0, ay=40, xref="x", yref="y",
                                           text='Verhoog schouw of BIS capaciteit', alignment='left',
                                           showarrow=True, arrowhead=2)] +
                                     [dict(x=20, y=50, ax=0, ay=40, xref="x", yref="y",
                                           text='Verhoog schouw of BIS capaciteit', alignment='left',
                                           showarrow=True, arrowhead=2)] +
                                     [dict(x=-23.5, y=135, ax=-100, ay=0, xref="x", yref="y",
                                           text='Verhoog HAS capaciteit',
                                           alignment='left', showarrow=True, arrowhead=2)] +
                                     [dict(x=-23.5, y=65, ax=-100, ay=0, xref="x", yref="y",
                                           text='Verruim afspraak KPN',
                                           alignment='left', showarrow=True, arrowhead=2)] +
                                     [dict(x=23.5, y=135, ax=100, ay=0, xref="x", yref="y",
                                           text='Verlaag HAS capcaciteit',
                                           alignment='right', showarrow=True, arrowhead=2)] +
                                     [dict(x=23.5, y=65, ax=100, ay=0, xref="x", yref="y",
                                           text='Verscherp afspraak KPN',
                                           alignment='right', showarrow=True, arrowhead=2)] +
                                     [dict(x=20, y=160, ax=0, ay=-40, xref="x", yref="y",
                                           text='Verlaag schouw of BIS capaciteit', alignment='left',
                                           showarrow=True, arrowhead=2)] +
                                     [dict(x=-20, y=160, ax=0, ay=-40, xref="x", yref="y",
                                           text='Verlaag schouw of BIS capaciteit', alignment='left',
                                           showarrow=True, arrowhead=2)],
                      'height': 500,
                      'width': 1700,
                      'margin': {'l': 60, 'r': 15, 'b': 40, 't': 40},
                      }
           }
    record = dict(id='project_performance', figure=fig)
    firestore.Client().collection('Graphs').document(record['id']).set(record)


def prognose_graph(x_d, y_prog_l, d_real_l, y_target_l):
    for key in y_prog_l:
        fig = {'data': [{
                         'x': list(x_d.strftime('%Y-%m-%d')),
                         'y': list(y_prog_l[key]),
                         'mode': 'lines',
                         'line': dict(color='rgb(200, 200, 0)'),
                         'name': 'Voorspelling (VQD)',
                         }],
               'layout': {
                          'xaxis': {'title': 'Opleverdatum [d]', 'range': ['2020-01-01', '2020-12-31']},
                          'yaxis': {'title': 'Opgeleverd HPend [%]', 'range': [0, 110]},
                          'title': {'text': 'Voortgang project vs outlook KPN:'},
                          'showlegend': True,
                          'legend': {'x': 1.2, 'xanchor': 'right', 'y': 1},
                          'height': 350
                           },
               }
        if key in d_real_l:
            fig['data'] = fig['data'] + [{
                                          'x': list(x_d[d_real_l[key].index.to_list()].strftime('%Y-%m-%d')),
                                          'y': d_real_l[key]['Aantal'].to_list(),
                                          'mode': 'markers',
                                          'line': dict(color='rgb(0, 200, 0)'),
                                          'name': 'Realisatie (FC)',
                                          }]

        if key in y_target_l:
            fig['data'] = fig['data'] + [{
                                          'x': list(x_d.strftime('%Y-%m-%d')),
                                          'y': list(y_target_l[key]),
                                          'mode': 'lines',
                                          'line': dict(color='rgb(170, 170, 170)'),
                                          'name': 'Outlook (KPN)',
                                          }]

        record = dict(id='project_' + key, figure=fig)
        firestore.Client().collection('Graphs').document(record['id']).set(record)


def info_table(tot_l, d_real_l, HP, y_target_l, x_d, HC_HPend_l, Schouw_BIS, HPend_l):
    n_now = int((pd.Timestamp.now() - pd.to_datetime('2019-12-30')).days / 7) + 1
    col = ['project', 'real vs KPN', 'real vs plan', 'HC / HP', 'Schouw & BIS gereed', 'HPend', 'woningen']
    records = []
    for key in d_real_l:
        if d_real_l[key].max()[0] < 100:
            record = dict(project=key)
            record['real vs KPN'] = round((d_real_l[key].max() - y_target_l[key][int((pd.Timestamp.now() -
                                          x_d[0]).days)]) / 100 * tot_l[key])[0]
            record['HC / HP'] = round(HC_HPend_l[key])
            if key in HP.keys():
                record['real vs plan'] = round(d_real_l[key].max() / 100 * tot_l[key] - sum(HP[key][:n_now]))[0]
            else:
                record['real vs plan'] = 0
            record['Schouw & BIS gereed'] = round(Schouw_BIS[key])
            record['HPend'] = round(HPend_l[key])
            # record['HAS gepland'] = round(len(df_l[key][~df_l[key].opleverdatum.isna()]))
            record['woningen'] = round(tot_l[key])
            records += [record]
    df_table = pd.DataFrame(records).to_json(orient='records')
    firestore.Client().collection('Graphs').document('info_table').set(dict(id='info_table', table=df_table, col=col))
