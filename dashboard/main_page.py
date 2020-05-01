import pandas as pd
# import numpy as np
import dash_core_components as dcc
# import plotly.graph_objs as go
import dash_html_components as html
# import dash_table
import time
import api
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
# from elements import table_styles
from app import app, cache

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
                                        "Status projecten FttX",
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
                            id='graph_targets_overall',
                            children=bar_projects(3),
                            className="pretty_container column",
                            hidden=False,
                    ),
                    html.Div(
                            id='graph_speed',
                            children=bar_projects(0),
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
                        [
                            html.H3("Details project:"),
                        ],
                        style={"margin-right": "140px"},
                    ),
                    html.Div(
                            [dcc.Dropdown(id='project-dropdown',
                                          options=bar_projects(2),
                                          value=None)],
                            className="pretty_container column",
                    ),
                ],
                className="container-display",
                id="title",
            ),
            html.Div(
                [
                    html.Div(
                            [dcc.Graph(id="graph_prog")],
                            id='subgraph1',
                            className="pretty_container column",
                            hidden=True,
                    ),
                    html.Div(
                            [dcc.Graph(id="graph_targets")],
                            id='subgraph4',
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
                            [dcc.Graph(id="Bar_LB")],
                            id='subgraph2',
                            className="pretty_container column",
                            hidden=True,
                    ),
                    html.Div(
                            [dcc.Graph(id="Bar_HB")],
                            id='subgraph3',
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
                        [
                            html.H3("Verdere details:"),
                        ],
                        id='text_table',
                        style={"margin-left": "42px"},
                        hidden=True,
                    ),
                    html.Div(
                        [dcc.Graph(id="geo_plot")],
                        id='subgraph6',
                        className="pretty_container column",
                        hidden=True,
                    ),
                ],
                className="container-display",
                id="title",
            ),
            html.Div(
                id='status_table_ext',
                className="pretty_container",
                hidden=True,
            ),
        ],
        id="mainContainer",
        style={"display": "flex", "flex-direction": "column"},
    )
    return page


# Globale grafieken
@app.callback(
    [Output("project-dropdown", 'value'),
     ],
    [Input("project_performance", 'clickData'),
     ]
)
def update_dropdown(value):
    return [value['points'][0]['text']]


# Globale grafieken
@app.callback(
    [
     Output("graph_prog", 'figure'),
     Output("graph_targets", 'figure'),
     Output("Bar_LB", "figure"),
     Output("Bar_HB", "figure"),
     Output("aggregate_data", 'data'),
     Output("aggregate_data2", 'data'),
     Output("graph_targets_overall", 'hidden'),
     Output("graph_speed", 'hidden'),
     Output("subgraph1", "hidden"),
     Output("subgraph2", "hidden"),
     Output("subgraph3", "hidden"),
     Output("subgraph4", "hidden"),
     ],
    [Input('project-dropdown', 'value'),
     Input("Bar_LB", 'clickData'),
     Input("Bar_HB", 'clickData'),
     ],
    [State("aggregate_data", 'data'),
     State("aggregate_data2", 'data'),
     ]
)
def project_plots(drop_selectie, cell_bar_LB, cell_bar_HB, mask_all, filter_a):
    if drop_selectie is None:
        raise PreventUpdate

    data = data_from_DB(drop_selectie, 0)
    data['bar_names'] = api.get('/plots_extra?id=bar_names')[0]

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
        if mask_all not in data['bar_names']:
            mask_all = '0'
    else:
        mask_all = '0'

    data['bar_filters'] = api.get('/plots_extra?id=' + drop_selectie + '_bar_filters_' + mask_all)[0]
    barLB, barHB, fig_prog, fig_bish = generate_mid_graphs(data['info_BISHAS'], data['info_prog'], data['bar_filters'], mask_all)

    hide_mid = False
    hide_top = True

    return [fig_prog, fig_bish, barLB, barHB, mask_all, drop_selectie, hide_top, hide_top, hide_mid, hide_mid, hide_mid, hide_mid]


# HELPER FUNCTIES
@cache.memoize()
def data_from_DB(pname, flag):
    t = time.time()
    data = {}

    if flag == 0:
        data['info_BISHAS'] = api.get('/plots_extra?id=' + pname)[0]
        data['info_prog'] = api.get('/plots_extra?id=' + 'project_' + pname)[0]

    if flag == 1:
        data['project_performance'] = api.get('/plot_overview_graphs?id=project_performance')[0]
        data['pnames'] = api.get('/plot_overview_graphs?id=pnames')[0]
        data['graph_targets'] = api.get('/plot_overview_graphs?id=graph_targets')[0]

    if flag == 2:
        df = pd.DataFrame()
        for i in range(0, 5):
            docs = api.get('/Projecten?id=' + pname + '_' + str(i))
            for doc in docs:
                df = df.append(pd.read_json(doc['df'], orient='records')).reset_index(drop=True)
        data['df'] = df

    print('time: ' + str(time.time() - t))

    return data


def bar_projects(s):

    data = data_from_DB(None, 1)

    if s == 0:
        fig = [dcc.Graph(id='project_performance',
                         figure=data['project_performance']['figure']
                         )]

    if s == 2:
        fig = data['pnames']['filters']

    if s == 3:
        w_now = int((pd.Timestamp.now() - pd.to_datetime('2019-12-30')).days / 7) + 1
        bar_t = [dict(x=[w_now],
                      y=[5000],
                      name='Huidige week',
                      type='bar',
                      marker=dict(color='rgb(0, 0, 0)'),
                      width=0.1,
                      )]
        data['graph_targets']['figure']['data'] = data['graph_targets']['figure']['data'] + bar_t
        data['graph_targets']['figure']['layout']['xaxis'] = {'range': [w_now - 5.5, w_now + 6.5],
                                                              'title': '[weken in 2020]'}
        fig = [dcc.Graph(id='graph_targets',
                         figure=data['graph_targets']['figure']
                         )]

    return fig


def generate_mid_graphs(info_BISHAS, info_prog, bar_filters, mask_all):
    bar = {}
    for key in bar_filters['bar']:
        bar[key] = [int(bar_filters['bar'][key])]

    labels = ['Schouwen', 'BIS', 'Montage-lasAP', 'Montage-lasDP', 'HAS']

    barLB1HC = dict(x=labels,
                    y=bar['SchouwenLB1'] + bar['BISLB1'] + bar['Montage-lasAPLB1'] + bar['Montage-lasDPLB1'] + bar['HASLB1'],
                    name='Opgeleverd (HAS: HC)',
                    type='bar',
                    marker=dict(color='rgb(0, 200, 0)'),
                    )
    barLB1HP = dict(x=labels,
                    y=[0] + [0] + [0] + [0] + bar['HASLB1HP'],
                    name='Opgeleverd (HAS: HP)',
                    type='bar',
                    marker=dict(color='rgb(0, 0, 200)')
                    )
    barLB0 = dict(x=labels,
                  y=bar['SchouwenLB0'] + bar['BISLB0'] + bar['Montage-lasAPLB0'] + bar['Montage-lasDPLB0'] + bar['HASLB0'],
                  name='Niet opgeleverd',
                  type='bar',
                  marker=dict(color='rgb(200, 0, 0)')
                  )
    barLB = dict(data=[barLB1HC, barLB1HP, barLB0],
                 layout=dict(barmode='stack',
                             clickmode='event+select',
                             showlegend=True,
                             height=350,
                             title={'text': 'Status per projectfase (LB & Duplex):', 'x': 0.5},
                             yaxis={'title': '[aantal woningen]'},
                             ))

    barHB1HC = dict(x=labels,
                    y=bar['SchouwenHB1'] + bar['BISHB1'] + bar['Montage-lasAPHB1'] + bar['Montage-lasDPHB1'] + bar['HASHB1'],
                    name='Opgeleverd (HAS: HC)',
                    type='bar',
                    marker=dict(color='rgb(0, 200, 0)')
                    )
    barHB1HP = dict(x=labels,
                    y=[0] + [0] + [0] + [0] + bar['HASHB1HP'],
                    name='Opgeleverd (HAS: HP)',
                    type='bar',
                    marker=dict(color='rgb(0, 0, 200)')
                    )
    barHB0 = dict(x=labels,
                  y=bar['SchouwenHB0'] + bar['BISHB0'] + bar['Montage-lasAPHB0'] + bar['Montage-lasDPHB0'] + bar['HASHB0'],
                  name='Niet opgeleverd',
                  type='bar',
                  marker=dict(color='rgb(200, 0, 0)')
                  )
    barHB = dict(data=[barHB1HC, barHB1HP, barHB0],
                 layout=dict(barmode='stack',
                             clickmode='event+select',
                             showlegend=True,
                             height=350,
                             title={'text': 'Status per projectfase (HB):', 'x': 0.5},
                             yaxis={'title': '[aantal woningen]'},
                             ))

    fig_prog = info_prog['figure']
    for i, item in enumerate(fig_prog['data']):
        fig_prog['data'][i]['x'] = pd.to_datetime(item['x'])

    fig_bish = info_BISHAS['figure']

    return barLB, barHB, fig_prog, fig_bish
