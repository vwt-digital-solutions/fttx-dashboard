import pandas as pd
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import dash_html_components as html
import dash_table
import time
import api
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from elements import table_styles
from app import app  # , cache

t_t = time.time()

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
                            # [dcc.Graph(figure=generate_graphs(4, None, None))],
                            # [dcc.Graph(figure=None)],
                            id='graph_targets_overall_c',
                            className="pretty_container column",
                            hidden=False,
                    ),
                    html.Div(
                            # [dcc.Graph(figure=generate_graphs(2, None, None),
                            # [dcc.Graph(figure=None,
                            #            id='project_performance')],
                            id='graph_speed_c',
                            className="pretty_container column",
                            hidden=False,
                    ),
                ],
                id="main_graphs0",
                className="container-display",
            ),
            html.Div(
                [
                    # html.Div(
                    #     [
                    #         html.H3("Details project:"),
                    #     ],
                    #     style={"margin-right": "140px"},
                    # ),
                    html.Div(
                            [dcc.Dropdown(id='project-dropdown',
                                          options=generate_graphs(3, None, None),
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
                            id='graph_prog_c',
                            className="pretty_container column",
                            hidden=True,
                    ),
                    html.Div(
                            [dcc.Graph(id="graph_targets")],
                            id='graph_targets_c',
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
                [dbc.Button('Project details', id='detail_button')],
            ),
            html.Div(
                [
                    html.Div(
                        id='table_c',
                        className="pretty_container",
                        hidden=True,
                    ),
                    html.Div(
                            [dcc.Graph(id="geo_plot")],
                            id='geo_plot_c',
                            className="pretty_container column",
                            hidden=True,
                            ),
                ],
                id="details",
                className="container-display",
            )
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


# update middle-top charts given dropdown selection
@app.callback(
    [
     Output("graph_prog", 'figure'),
     Output("graph_targets", 'figure'),
     Output("graph_targets_overall_c", 'hidden'),
     Output("graph_speed_c", 'hidden'),
     Output("graph_prog_c", "hidden"),
     Output("graph_targets_c", "hidden"),
     ],
    [
     Input('project-dropdown', 'value'),
     ],
)
def middle_top_graphs(drop_selectie):
    if drop_selectie is None:
        raise PreventUpdate

    t = time.time()
    fig_bish = generate_graphs(0, drop_selectie, None)
    fig_prog = generate_graphs(1, drop_selectie, None)
    print('time middle_top_graphs: ' + str(time.time() - t))

    return [fig_prog, fig_bish, True, True, False, False]


# update click bar charts
@app.callback(
    [
     Output("Bar_LB", "figure"),
     Output("Bar_HB", "figure"),
     Output("aggregate_data", 'data'),
     Output("aggregate_data2", 'data'),
     Output("Bar_LB_c", "hidden"),
     Output("Bar_HB_c", "hidden"),
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
    t = time.time()
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

        doc = api.get('/plots_extra?id=bar_names')[0]['bar_names']
        if mask_all not in doc:
            mask_all = '0'
    else:
        mask_all = '0'

    barLB = generate_graphs(5, drop_selectie, mask_all)
    barHB = generate_graphs(6, drop_selectie, mask_all)
    print('time clickbars: ' + str(time.time() - t))
    return [barLB, barHB, mask_all, drop_selectie, False, False]

    Output("uitleg_collapse", "hidden"),


# update geomap
@app.callback(
    [
     Output("geo_plot", 'figure'),
     Output("table_c", 'children'),
     Output("geo_plot_c", "hidden"),
     Output("table_c", "hidden"),
     ],
    [
     Input("detail_button", "n_clicks")
     ],
    [
     State('project-dropdown', 'value'),
     State("geo_plot_c", "hidden"),
     State("aggregate_data", 'data'),
     ],
)
def geomap(n, drop_selectie, hidden, mask_all):
    if (drop_selectie is None) | (mask_all is None):
        raise PreventUpdate
    if n:
        hidden = not hidden
    t = time.time()
    fig = generate_graphs(7, drop_selectie, mask_all)
    print('time geomap: ' + str(time.time() - t))
    return [fig['geo'], fig['table'], hidden, hidden]


# HELPER FUNCTIES
# @cache.memoize()
def generate_graphs(flag, drop_selectie, mask_all):

    # BIS/HAS
    if flag == 0:
        fig = api.get('/plots_extra?id=' + drop_selectie)[0]['figure']

    # prognose
    if flag == 1:
        fig = api.get('/plots_extra?id=' + 'project_' + drop_selectie)[0]['figure']
        for i, item in enumerate(fig['data']):
            fig['data'][i]['x'] = pd.to_datetime(item['x'])

    # project speed
    if flag == 2:
        t = time.time()
        fig = api.get('/plot_overview_graphs?id=project_performance')[0]['figure']
        print('time speed: ' + str(time.time() - t))

    # labels
    if flag == 3:
        t = time.time()
        fig = api.get('/plot_overview_graphs?id=pnames')[0]['filters']
        print('time labels: ' + str(time.time() - t))

    # targets
    if flag == 4:
        t = time.time()
        fig = api.get('/plot_overview_graphs?id=graph_targets')[0]['figure']
        w_now = int((pd.Timestamp.now() - pd.to_datetime('2019-12-30')).days / 7) + 1
        bar_t = [dict(x=[w_now],
                      y=[5000],
                      name='Huidige week',
                      type='bar',
                      marker=dict(color='rgb(0, 0, 0)'),
                      width=0.1,
                      )]
        fig['data'] = fig['data'] + bar_t
        fig['layout']['xaxis'] = {'range': [w_now - 5.5, w_now + 6.5], 'title': '[weken in 2020]'}
        print('time targets: ' + str(time.time() - t))

    # clickbar LB
    if flag == 5:
        fig = api.get('/plots_extra?id=' + drop_selectie + '_bar_filters_' + mask_all)[0]['bar']
        bar = {}
        for key in fig:
            if 'LB' in key:
                bar[key] = [int(fig[key])]
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
        fig = dict(data=[barLB1HC, barLB1HP, barLB0],
                   layout=dict(barmode='stack',
                               clickmode='event+select',
                               showlegend=True,
                               height=350,
                               title={'text': 'Status per projectfase (LB & Duplex):', 'x': 0.5},
                               yaxis={'title': '[aantal woningen]'},
                               ))

    # clickbar HB
    if flag == 6:
        fig = api.get('/plots_extra?id=' + drop_selectie + '_bar_filters_' + mask_all)[0]['bar']
        bar = {}
        for key in fig:
            if 'HB' in key:
                bar[key] = [int(fig[key])]
        labels = ['Schouwen', 'BIS', 'Montage-lasAP', 'Montage-lasDP', 'HAS']
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
        fig = dict(data=[barHB1HC, barHB1HP, barHB0],
                   layout=dict(barmode='stack',
                               clickmode='event+select',
                               showlegend=True,
                               height=350,
                               title={'text': 'Status per projectfase (HB):', 'x': 0.5},
                               yaxis={'title': '[aantal woningen]'},
                               ))

    # geomap & data table
    if flag == 7:
        df = pd.DataFrame()
        for i in range(0, 5):
            docs = api.get('/Projecten?id=' + drop_selectie + '_' + str(i))
            for doc in docs:
                df = df.append(pd.read_json(doc['df'], orient='records')).reset_index(drop=True)
        print(mask_all)
        if mask_all != '0':
            df = df[api.get('/plots_extra?id=' + drop_selectie + '_bar_filters_' + mask_all)[0]['mask']]

        if not df[~df['X locatie Rol'].isna()].empty:
            df['clr'] = 0
            df.loc[~df['Opleverdatum'].isna(), ('clr')] = 50
            df['clr-DP'] = 0
            df.loc[df['Opleverstatus'] != 0, ('clr-DP')] = 25
            df['X locatie Rol'] = df['X locatie Rol'].str.replace(',', '.').astype(float)
            df['Y locatie Rol'] = df['Y locatie Rol'].str.replace(',', '.').astype(float)
            df['X locatie DP'] = df['X locatie DP'].str.replace(',', '.').astype(float)
            df['Y locatie DP'] = df['Y locatie DP'].str.replace(',', '.').astype(float)
            df['Lat'], df['Long'] = from_rd(df['X locatie Rol'], df['Y locatie Rol'])
            df['Lat_DP'], df['Long_DP'] = from_rd(df['X locatie DP'], df['Y locatie DP'])
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
                margin=dict(r=30, b=20, t=100),
                height=600,
                hovermode="closest",
                plot_bgcolor="#F9F9F9",
                paper_bgcolor="#F9F9F9",
                legend=dict(font=dict(size=10), orientation="h"),
                title="Status woningen [klein, groen = opgeleverd]<br>Status DP's [groot, geel = opgeleverd]",
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

        df['Uitleg RedenNA'] = df['RedenNA'].map(api.get('/plots_extra?id=reden_mapping')[0]['map'])
        df = df[['Sleutel', 'Opleverdatum', 'HASdatum', 'Opleverstatus', 'Uitleg RedenNA']].sort_values(by='HASdatum')
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

    return fig


def from_rd(x: int, y: int) -> tuple:
    t = time.time()
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
    print('time from_rd: ' + str(time.time() - t))
    return latitude, longitude


print('time main_page.py: ' + str(time.time() - t_t))
