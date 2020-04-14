import pandas as pd
import numpy as np
import dash_core_components as dcc
import plotly.graph_objs as go
import dash_html_components as html
import dash_table
import time
import api
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from elements import table_styles
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
                    html.Div(
                            [dcc.Graph(id="geo_plot")],
                            id='subgraph6',
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
                    html.Div(
                            [dcc.Graph(id="count_R")],
                            id='subgraph5',
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
                    )
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


def bar_projects(s):

    _, _, _, _, doc, _, _, _, _ = data_from_DB(None, 1)

    if s == 0:
        fig = [dcc.Graph(id='project_performance',
                         figure={'data': [{
                                           'x': doc['x1'],
                                           'y': doc['y1'],
                                           'name': 'Trace 2',
                                           'mode': 'lines',
                                           'fill': 'toself',
                                           'line': {'color': 'rgb(0, 200, 0)'}
                                           },
                                          {
                                           'x': doc['x2'],
                                           'y': doc['y2'],
                                           'text': doc['pnames'],
                                           'name': 'Trace 1',
                                           'mode': 'markers',
                                           'marker': {'size': 15}
                                           },
                                          ],
                                 'layout': {'clickmode': 'event+select',
                                            'xaxis': {'title': 'opgeleverde woningen (HP & HC) [%]'},
                                            'yaxis': {'title': 'gemiddelde snelheid [woningen / dag]'},
                                            'showlegend': False,
                                            'title':
                                            {'text': 'Klik op een project voor meer informatie!<br>' +
                                                '[Het groene vlak geeft 75% tot 125% van het gemiddelde aan]'},
                                            }
                                 }
                         )]

    if s == 2:
        filters = []
        for el in doc['pnames']:
            filters += [{'label': el, 'value': el}]
        fig = filters

    if s == 3:
        w_now = int((pd.Timestamp.now() - pd.to_datetime('2019-12-30')).days / 7) + 1
        bar_p = go.Bar(x=[el - 0.3 for el in doc['x_target']],
                       y=doc['p_target'],
                       name='Targets (KPN)',
                       marker=go.bar.Marker(color='rgb(200, 0, 0)'),
                       width=0.2,
                       )
        bar_y = go.Bar(x=[el - 0.1 for el in doc['x_target']],
                       y=doc['y_target'],
                       name='Prognose (VQD)',
                       marker=go.bar.Marker(color='rgb(200, 200, 0)'),
                       width=0.2,
                       )
        bar_k = go.Bar(x=[el + 0.1 for el in doc['x_target']],
                       y=doc['k_target'],
                       name='Realisatie (FC)',
                       marker=go.bar.Marker(color='rgb(0, 200, 0)'),
                       width=0.2,
                       )
        bar_z = go.Bar(x=[el + 0.3 for el in doc['x_target']],
                       y=doc['z_target'],
                       name='Realisatie / Planning (VWT)',
                       marker=go.bar.Marker(color='rgb(0, 0, 200)'),
                       width=0.2,
                       )
        bar_t = go.Bar(x=[w_now],
                       y=[5000],
                       name='Huidige week',
                       marker=go.bar.Marker(color='rgb(0, 0, 0)'),
                       width=0.1,
                       )
        fig = [dcc.Graph(id="graph_targets",
                         figure=go.Figure(data=[bar_z, bar_k, bar_y, bar_p, bar_t],
                                          layout=go.Layout(barmode='stack',
                                                           clickmode='event+select',
                                                           showlegend=True,
                                                           #    legend=dict(x=0.75, y=1.1),
                                                           title={'text': 'Aantal opgeleverde woningen (HP & HC):',
                                                                  'x': 0.5},
                                                           xaxis={'range': [w_now - 5.5, w_now + 6.5], 'title': '[weken in 2020]'},
                                                           yaxis={'range': [0, 5000], 'title': '[aantal woningen]'},
                                                           )
                                          )
                         )
               ]

    return fig


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
    [Output("Bar_LB", "figure"),
     Output("Bar_HB", "figure"),
     Output("status_table_ext", "children"),
     Output("status_table_ext", "hidden"),
     Output("subgraph1", "hidden"),
     #  Output("subgraph1b", "hidden"),
     Output("subgraph2", "hidden"),
     Output("subgraph3", "hidden"),
     Output("subgraph4", "hidden"),
     Output("subgraph5", "hidden"),
     Output("subgraph6", "hidden"),
     Output("text_table", "hidden"),
     Output("geo_plot", "figure"),
     Output("count_R", "figure"),
     Output("aggregate_data", 'data'),
     Output("aggregate_data2", 'data'),
     Output("graph_prog", 'figure'),
     #  Output("graph_BIS-HAS", 'figure'),
     Output("graph_targets", 'figure'),
     Output("graph_targets_overall", 'hidden'),
     Output("graph_speed", 'hidden'),
     ],
    [Input('project-dropdown', 'value'),
     Input("Bar_LB", 'clickData'),
     Input("Bar_HB", 'clickData'),
     Input("count_R", 'clickData'),
     ],
    [State("aggregate_data", 'data'),
     State("aggregate_data2", 'data'),
     ]
)
def make_barplot(drop_selectie, cell_b1, cell_b2, cell_bR, mask_all, filter_a):
    if (drop_selectie is None):
        raise PreventUpdate
    print(drop_selectie)
    df_l, t_s, x_e, x_d, _, date_FTU0, date_FTU1, info_BISHAS, info_prog = data_from_DB(drop_selectie, 0)
    df = df_l[drop_selectie]
    hidden = True
    hidden2 = False
    hidden3 = True

    if (drop_selectie == filter_a) & ((cell_b1 is not None) | (cell_b2 is not None)):
        hidden = False
        if cell_b1 is not None:
            pt_f = cell_b1['points'][0]['x']
            if cell_b1['points'][0]['curveNumber'] == 0:
                pt_c = 'LB1'
            if cell_b1['points'][0]['curveNumber'] == 1:
                pt_c = 'LB1HP'
            if cell_b1['points'][0]['curveNumber'] == 2:
                pt_c = 'LB0'
        if cell_b2 is not None:
            pt_f = cell_b2['points'][0]['x']
            if cell_b2['points'][0]['curveNumber'] == 0:
                pt_c = 'HB1'
            if cell_b2['points'][0]['curveNumber'] == 1:
                pt_c = 'HB1HP'
            if cell_b2['points'][0]['curveNumber'] == 2:
                pt_c = 'HB0'

        bar, _, _, _ = processed_data(df)
        mask = bar[pt_f + pt_c + '-mask']

        if mask_all is None:
            mask_all = mask
        else:
            mask_all = mask_all & mask

        df = df[mask_all]

    else:
        mask_all = None

    if df.empty:
        raise PreventUpdate

    barLB, barHB, stats, geo_plot, df_table, bar_R, fig_prog, fig_bish = \
        generate_graph(df, drop_selectie, info_BISHAS, info_prog)

    return [barLB, barHB, df_table, hidden, hidden2, hidden2, hidden2, hidden2, hidden2, hidden2, hidden,
            geo_plot, bar_R, mask_all, drop_selectie, fig_prog, fig_bish, hidden3, hidden3]


# HELPER FUNCTIES
@cache.memoize()
def data_from_DB(pname, flag):

    t = time.time()

    if flag == 0:
        df = pd.DataFrame()
        for i in range(0, 5):
            url_s = '/Projecten?id=' + pname + '_' + str(i)
            docs = api.get(url_s)
            for doc in docs:
                df = df.append(pd.read_json(doc['df'], orient='records')).reset_index(drop=True)
        df_l = {}
        df_l[pname] = df

        url_s = '/plots_extra?id=dates_FTU'
        doc = api.get(url_s)
        date_FTU0 = doc[0]['date_FTU0']
        date_FTU1 = doc[0]['date_FTU1']
        t_s = doc[0]['t_s']
        for key in t_s:
            t_s[key] = pd.to_datetime(t_s[key])
        t_d = doc[0]['t_d']
        x_e = np.array(list(range(0, t_d + 1)))
        x_d = pd.date_range(min(t_s.values()), periods=t_d + 1, freq='D')
        print(min(t_s.values()))

        url_s = '/plots_extra?id=' + pname
        doc = api.get(url_s)
        info_BISHAS = doc[0]

        url_s = '/plots_extra?id=' + 'project_' + pname
        doc = api.get(url_s)
        info_prog = doc[0]

        plot_parameters = None

    if flag == 1:
        url_s = '/plot_overview_graphs?id=plot_parameters'
        doc = api.get(url_s)
        plot_parameters = doc[0]
        df_l = None
        t_s = None
        x_e = None
        x_d = None
        date_FTU0 = None
        date_FTU1 = None
        info_BISHAS = None
        info_prog = None

    print('time: ' + str(time.time() - t))

    return df_l, t_s, x_e, x_d, plot_parameters, date_FTU0, date_FTU1, info_BISHAS, info_prog


def generate_graph(df, filter_selectie, info_BISHAS, info_prog):

    bar, stats, df_g, count_R = processed_data(df)

    if bar is not None:
        reden_l = dict(
            R0='Geplande aansluiting',
            R00='Geplande aansluiting',
            R1='Geen toestemming bewoner',
            R01='Geen toestemming bewoner',
            R2='Geen toestemming VVE / WOCO',
            R02='Geen toestemming VVE / WOCO',
            R3='Bewoner na 3 pogingen niet thuis',
            R4='Nieuwbouw (woning nog niet gereed)',
            R5='Hoogbouw obstructie (blokkeert andere bewoners)',
            R6='Hoogbouw obstructie (wordt geblokkeerd door andere bewoners)',
            R7='Technische obstructie',
            R8='Meterkast voldoet niet aan eisen',
            R9='Pand staat leeg',
            R10='Geen graafvergunning',
            R11='Aansluitkosten boven normbedrag niet gedekt',
            R12='Buiten het uitrolgebied',
            R13='Glasnetwerk van een andere operator',
            R14='Geen vezelcapaciteit',
            R15='Geen woning',
            R16='Sloopwoning (niet voorbereid)',
            R17='Complex met 1 aansluiting op ander adres',
            R18='Klant niet bereikbaar',
            R19='Bewoner niet thuis, wordt opnieuw ingepland',
            R20='Uitrol na vraagbundeling, klant neemt geen dienst',
            R21='Wordt niet binnen dit project aangesloten',
            R22='Vorst, niet planbaar',
            R_geen='Geen reden'
            )
        labels = {}
        labels['OHW'] = ['Schouwen', 'BIS', 'Montage-lasAP',
                         'Montage-lasDP', 'HAS']

        bar1a = go.Bar(x=labels['OHW'],
                       y=bar['SchouwenLB1'] +
                       bar['BISLB1'] +
                       bar['Montage-lasAPLB1'] +
                       bar['Montage-lasDPLB1'] +
                       bar['HASLB1'],
                       name='Opgeleverd (HAS: HC)',
                       marker=go.bar.Marker(color='rgb(0, 200, 0)'))
        bar1b = go.Bar(x=labels['OHW'],
                       y=[0] +
                       [0] +
                       [0] +
                       [0] +
                       bar['HASLB1HP'],
                       name='Opgeleverd (HAS: HP)',
                       marker=go.bar.Marker(color='rgb(0, 0, 200)'))
        bar1c = go.Bar(x=labels['OHW'],
                       y=bar['SchouwenLB0'] +
                       bar['BISLB0'] +
                       bar['Montage-lasAPLB0'] +
                       bar['Montage-lasDPLB0'] +
                       bar['HASLB0'],
                       name='Niet opgeleverd',
                       marker=go.bar.Marker(color='rgb(200, 0, 0)'))
        barLB = go.Figure(data=[bar1a, bar1b, bar1c],
                          layout=go.Layout(barmode='stack',
                                           clickmode='event+select',
                                           showlegend=True,
                                           height=350,
                                           title={'text': 'Status per projectfase (LB & Duplex):',
                                                  'x': 0.5},
                                           yaxis={'title': '[aantal woningen]'},
                                           ))

        bar1d = go.Bar(x=labels['OHW'],
                       y=bar['SchouwenHB1'] +
                       bar['BISHB1'] +
                       bar['Montage-lasAPHB1'] +
                       bar['Montage-lasDPHB1'] +
                       bar['HASHB1'],
                       name='Opgeleverd (HAS: HC)',
                       marker=go.bar.Marker(color='rgb(0, 200, 0)'))
        bar1e = go.Bar(x=labels['OHW'],
                       y=[0] +
                       [0] +
                       [0] +
                       [0] +
                       bar['HASHB1HP'],
                       name='Opgeleverd (HAS: HP)',
                       marker=go.bar.Marker(color='rgb(0, 0, 200)'))
        bar1f = go.Bar(x=labels['OHW'],
                       y=bar['SchouwenHB0'] +
                       bar['BISHB0'] +
                       bar['Montage-lasAPHB0'] +
                       bar['Montage-lasDPHB0'] +
                       bar['HASHB0'],
                       name='Niet opgeleverd',
                       marker=go.bar.Marker(color='rgb(200, 0, 0)'))
        barHB = go.Figure(data=[bar1d, bar1e, bar1f],
                          layout=go.Layout(barmode='stack',
                                           clickmode='event+select',
                                           showlegend=True,
                                           height=350,
                                           title={'text': 'Status per projectfase (HB):',
                                                  'x': 0.5},
                                           yaxis={'title': '[aantal woningen]'},
                                           ))

        df_t = df.copy()
        df_t['Uitleg RedenNA'] = df_t['RedenNA'].map(reden_l)
        df_t = df_t[['Sleutel', 'Opleverdatum', 'HASdatum', 'Opleverstatus', 'Uitleg RedenNA']]
        df_table = dash_table.DataTable(
            columns=[{"name": i, "id": i} for i in df_t.columns],
            data=df_t.to_dict("rows"),
            style_table={'overflowX': 'auto'},
            style_header=table_styles['header'],
            style_cell=table_styles['cell']['action'],
            style_filter=table_styles['filter'],
            css=[{
                'selector': 'table',
                'rule': 'width: 100%;'
            }],
        )

        count_Ra = dict(Wachten_op_actie=0, Niet_aansluiten=0, Geen_obstructies=0)
        for key in count_R.keys():
            if key in ['R4', 'R5', 'R6', 'R7', 'R8', 'R9', 'R10', 'R11', 'R12', 'R13', 'R14', 'R17', 'R18', 'R19', 'R22']:
                count_Ra['Wachten_op_actie'] = count_Ra['Wachten_op_actie'] + count_R[key]
            if key in ['R1', 'R01', 'R2', 'R02', 'R3', 'R15', 'R16', 'R20', 'R21']:
                count_Ra['Niet_aansluiten'] = count_Ra['Niet_aansluiten'] + count_R[key]
            if key in ['R0', 'R00', 'R_geen']:
                count_Ra['Geen_obstructies'] = count_Ra['Geen_obstructies'] + count_R[key]

        layout = dict(
            autosize=True,
            automargin=True,
            margin=dict(le=30, r=30, b=20, t=40),
            hovermode="closest",
            height=350,
            legend=dict(font=dict(size=14), orientation="h"),
        )

        data_pie = [
            dict(
                type="pie",
                labels=list(count_Ra.keys()),
                values=list(count_Ra.values()),
                hoverinfo="percent",
                textinfo="value",
                hole=0.5,
                domain={"x": [0, 0.5], "y": [0.25, 0.75]},
                sort=False
            )
        ]
        layout_pie = layout
        layout_pie["title"] = {'text': "Redenen bij woningen:",
                               'y': 0.92,
                               }
        layout_pie["legend"] = dict(
            orientation="v",
            traceorder='normal',
            x=0.5,
            y=0.5
        )
        layout_pie["showlegend"] = True
        bar_R = dict(data=data_pie, layout=layout_pie)

        fig_prog = {'data': [{
                              'x': pd.to_datetime(info_prog['x_e_l']),
                              'y': info_prog['y_e_l'],
                              'mode': 'lines',
                              'line': dict(color='rgb(200, 200, 0)'),
                              'name': 'Prognose (VQD)',
                              },
                             ],
                    'layout': {
                               'xaxis': {'title': 'Opleverdatum [d]',
                                         'range': info_prog['x_range'],
                                         },
                               'yaxis': {'title': 'Niet opgeleverde woningen [%]',
                                         'range': [0, 110]
                                         },
                               'title': {'text': 'Trends in oplevering project:'},
                               'showlegend': True,
                               'height': 350,
                               }
                    }
        if 'df_s_l' in info_prog:
            fig_prog['data'] = fig_prog['data'] + [{
                                                    'x': pd.to_datetime(info_prog['df_s_l_i']),
                                                    'y': info_prog['df_s_l'],
                                                    'mode': 'markers',
                                                    'line': dict(color='rgb(0, 200, 0)'),
                                                    'name': 'Realisatie (FC)',
                                                    }]
        if 'y_t_l' in info_prog:
            fig_prog['data'] = fig_prog['data'] + [{
                                                    'x': pd.to_datetime(info_prog['x_t_l']),
                                                    'y': info_prog['y_t_l'],
                                                    'mode': 'lines',
                                                    'line': dict(color='rgb(200, 0, 0)'),
                                                    'name': 'Target (KPN)',
                                                    }]

        if ('y_KPN' in info_BISHAS) & ('y_BIS' in info_BISHAS):
            if len(info_BISHAS['y_KPN']) > 0:
                advies = 'Advies:<br>' + info_BISHAS['Advies_BIS'] + ' BIS teams<br>' + info_BISHAS['Advies_HAS'] + ' HAS teams'
            else:
                advies = 'FTU data niet bekend'
        else:
            advies = 'Geen FTU-1 of FTU-eind bekend'
        if 'x_comp' in info_BISHAS:
            fig_bish = {'data': [{
                                 'x': info_BISHAS['x_comp'],
                                 'y': info_BISHAS['y_schouw'],
                                 'mode': 'markers',
                                 'marker': {'symbol': 'triangle-down', 'color': 'rgb(0, 200, 0)'},
                                 'name': 'Geschouwd',
                                 }] +
                                [{
                                 'x': info_BISHAS['x_comp'],
                                 'y': info_BISHAS['y_BIS'],
                                 'mode': 'markers',
                                 'marker': {'symbol': 'x', 'color': 'rgb(0, 200, 0)'},
                                 'name': 'BIS-gegraven',
                                 }] +
                                [{
                                 'x': info_BISHAS['x_comp'],
                                 'y': info_BISHAS['y_HASm'],
                                 'mode': 'markers',
                                 'marker': {'symbol': 'circle', 'color': 'rgb(0, 200, 0)'},
                                 'name': 'HAS-gegraven',
                                 }] +
                                [{
                                 'x': info_BISHAS['x_comp'],
                                 'y': info_BISHAS['y_HAS'],
                                 'mode': 'markers',
                                 'marker': {'symbol': 'diamond', 'color': 'rgb(0, 200, 0)'},
                                 'name': 'HAS-opgeleverd (HP / 2)',
                                 }],
                        'layout': {
                                'xaxis': {'title': '[weken in 2020]',
                                          'range': [0, 21],
                                          },
                                'yaxis': {'title': 'Niet opgeleverde woningen [%]',
                                          'range': [0, 110]
                                          },
                                'title': {'text': 'Trends in oplevering projectfases:'},
                                'showlegend': True,
                                # 'legend': dict(loc=1),
                                'height': 350,
                                'annotations': [dict(x=10, y=20, text=advies, xref="x", yref="y", ax=0, ay=0, alignment='left')],
                                }
                        }
            if 'y_KPN' in info_BISHAS:
                if len(info_BISHAS['y_KPN']) > 0:
                    fig_bish['data'] = fig_bish['data'] + [{
                                                            'x': info_BISHAS['x_KPN'],
                                                            'y': info_BISHAS['y_KPN'],
                                                            'mode': 'line',
                                                            'line': dict(color='rgb(200, 0, 0)'),
                                                            'name': 'Targets (KPN)',
                                                            }]
        else:
            fig_bish = {
                        'layout': {
                                'xaxis': {'title': ' ',
                                          'range': [0, 21],
                                          },
                                'yaxis': {'title': ' ',
                                          'range': [0, 110]
                                          },
                                'title': {'text': ' '},
                                'showlegend': False,
                                'height': 350,
                                }
            }

    if df_g is not None:
        # this is a default public token obtained from a free account on https://account.mapbox.com/
        # and can there be refreshed at any moment
        mapbox_at = 'pk.eyJ1IjoiYXZhbnR1cm5ob3V0IiwiYSI6ImNrOGl4Y2o3ZTA5MjMzbW53a3dicTRnMnIifQ.FdFexMQbqQrZBNMEZkYvvg'
        normalized_size = df_g['Size_DP'].to_list() + df_g['Size'].to_list()

        map_data = [
            go.Scattermapbox(
                lat=df_g['Lat_DP'].to_list() + df_g['Lat'].to_list(),
                lon=df_g['Long_DP'].to_list() + df_g['Long'].to_list(),
                mode='markers',
                marker=dict(
                    cmax=50,
                    cmin=0,
                    color=df_g['clr-DP'].to_list() + df_g['clr'].to_list(),
                    colorscale=['green', 'yellow', 'red'],
                    reversescale=True,
                    size=normalized_size * 7,
                ),
                text=df_g['clr'],
                hoverinfo='text'
            )
        ]

        map_layout = dict(
            autosize=True,
            automargin=True,
            margin=dict(r=30, b=20, t=100),
            height=350,
            hovermode="closest",
            plot_bgcolor="#F9F9F9",
            paper_bgcolor="#F9F9F9",
            legend=dict(font=dict(size=10), orientation="h"),
            title="Status woningen [klein, groen = opgeleverd]<br>Status DP's [groot, geel = opgeleverd]",
            mapbox=dict(
                accesstoken=mapbox_at,
                style="light",
                center=dict(lon=df_g['Long'].mean(), lat=df_g['Lat'].mean()),
                zoom=13,
            ),
        )

        geo_plot = {'data': map_data, 'layout': map_layout}
    else:
        geo_plot = {'data': None, 'layout': dict()}

    return barLB, barHB, stats, geo_plot, df_table, bar_R, fig_prog, fig_bish


def processed_data(df):

    bar = {}
    bar['SchouwenLB0-mask'] = (df['Toestemming'].isna()) & \
                              (df['Soort_bouw'] != 'Hoog')
    bar['SchouwenLB0'] = [len(df[bar['SchouwenLB0-mask']])]
    bar['SchouwenLB1-mask'] = (~df['Toestemming'].isna()) & \
                              (df['Soort_bouw'] != 'Hoog')
    bar['SchouwenLB1'] = [len(df[bar['SchouwenLB1-mask']])]
    bar['SchouwenHB0-mask'] = (df['Toestemming'].isna()) & \
                              (df['Soort_bouw'] == 'Hoog')
    bar['SchouwenHB0'] = [len(df[bar['SchouwenHB0-mask']])]
    bar['SchouwenHB1-mask'] = (~df['Toestemming'].isna()) &\
                              (df['Soort_bouw'] == 'Hoog')
    bar['SchouwenHB1'] = [len(df[bar['SchouwenHB1-mask']])]

    bar['BISLB0-mask'] = (df['Opleverstatus'] == 0) & \
                         (df['Soort_bouw'] != 'Hoog')
    bar['BISLB0'] = [len(df[bar['BISLB0-mask']])]
    bar['BISLB1-mask'] = (df['Opleverstatus'] != 0) & \
                         (df['Soort_bouw'] != 'Hoog')
    bar['BISLB1'] = [len(df[bar['BISLB1-mask']])]
    bar['BISHB0-mask'] = (df['Opleverstatus'] == 0) & \
                         (df['Soort_bouw'] == 'Hoog')
    bar['BISHB0'] = [len(df[bar['BISHB0-mask']])]
    bar['BISHB1-mask'] = (df['Opleverstatus'] != 0) & \
                         (df['Soort_bouw'] == 'Hoog')
    bar['BISHB1'] = [len(df[bar['BISHB1-mask']])]

    bar['Montage-lasDPLB0-mask'] = (df['LaswerkDPGereed'] == 0) & \
                                   (df['Soort_bouw'] != 'Hoog')
    bar['Montage-lasDPLB0'] = [len(df[bar['Montage-lasDPLB0-mask']])]
    bar['Montage-lasDPLB1-mask'] = (df['LaswerkDPGereed'] == 1) & \
                                   (df['Soort_bouw'] != 'Hoog')
    bar['Montage-lasDPLB1'] = [len(df[bar['Montage-lasDPLB1-mask']])]
    bar['Montage-lasDPHB0-mask'] = (df['LaswerkDPGereed'] == 0) & \
                                   (df['Soort_bouw'] == 'Hoog')
    bar['Montage-lasDPHB0'] = [len(df[bar['Montage-lasDPHB0-mask']])]
    bar['Montage-lasDPHB1-mask'] = (df['LaswerkDPGereed'] == 1) & \
                                   (df['Soort_bouw'] == 'Hoog')
    bar['Montage-lasDPHB1'] = [len(df[bar['Montage-lasDPHB1-mask']])]

    bar['Montage-lasAPLB0-mask'] = (df['LaswerkAPGereed'] == 0) & \
                                   (df['Soort_bouw'] != 'Hoog')
    bar['Montage-lasAPLB0'] = [len(df[bar['Montage-lasAPLB0-mask']])]
    bar['Montage-lasAPLB1-mask'] = (df['LaswerkAPGereed'] == 1) & \
                                   (df['Soort_bouw'] != 'Hoog')
    bar['Montage-lasAPLB1'] = [len(df[bar['Montage-lasAPLB1-mask']])]
    bar['Montage-lasAPHB0-mask'] = (df['LaswerkAPGereed'] == 0) & \
                                   (df['Soort_bouw'] == 'Hoog')
    bar['Montage-lasAPHB0'] = [len(df[bar['Montage-lasAPHB0-mask']])]
    bar['Montage-lasAPHB1-mask'] = (df['LaswerkAPGereed'] == 1) & \
                                   (df['Soort_bouw'] == 'Hoog')
    bar['Montage-lasAPHB1'] = [len(df[bar['Montage-lasAPHB1-mask']])]

    bar['HASLB0-mask'] = (df['Opleverdatum'].isna()) & \
                         (df['Soort_bouw'] != 'Hoog')
    bar['HASLB0'] = [len(df[bar['HASLB0-mask']])]
    bar['HASLB1-mask'] = (df['Opleverstatus'] == 2) & \
                         (df['Soort_bouw'] != 'Hoog')
    bar['HASLB1'] = [len(df[bar['HASLB1-mask']])]
    bar['HASLB1HP-mask'] = (df['Opleverstatus'] != 2) & \
                           (~df['Opleverdatum'].isna()) & \
                           (df['Soort_bouw'] != 'Hoog')
    bar['HASLB1HP'] = [len(df[bar['HASLB1HP-mask']])]
    bar['HASHB0-mask'] = (df['Opleverdatum'].isna()) & \
                         (df['Soort_bouw'] == 'Hoog')
    bar['HASHB0'] = [len(df[bar['HASHB0-mask']])]
    bar['HASHB1-mask'] = (df['Opleverstatus'] == 2) & \
                         (df['Soort_bouw'] == 'Hoog')
    bar['HASHB1'] = [len(df[bar['HASHB1-mask']])]
    bar['HASHB1HP-mask'] = (df['Opleverstatus'] != 2) & \
                           (~df['Opleverdatum'].isna()) & \
                           (df['Soort_bouw'] == 'Hoog')
    bar['HASHB1HP'] = [len(df[bar['HASHB1HP-mask']])]

    stats = {'0': str(round(len(df))),
             '1': str(round(0)),
             '2': str(round(0))}

    df_g = df.copy()
    if df_g[~df_g['X locatie Rol'].isna()].empty:
        df_g = None
    else:
        df_g['clr'] = 0
        df_g.loc[~df_g['Opleverdatum'].isna(), ('clr')] = 50
        df_g['clr-DP'] = 0
        df_g.loc[df_g['Opleverstatus'] != 0, ('clr-DP')] = 25
        df_g['X locatie Rol'] = df_g['X locatie Rol'].str.replace(
            ',', '.').astype(float)
        df_g['Y locatie Rol'] = df_g['Y locatie Rol'].str.replace(
            ',', '.').astype(float)
        df_g['X locatie DP'] = df_g['X locatie DP'].str.replace(
            ',', '.').astype(float)
        df_g['Y locatie DP'] = df_g['Y locatie DP'].str.replace(
            ',', '.').astype(float)
        df_g['Lat'], df_g['Long'] = from_rd(df_g['X locatie Rol'],
                                            df_g['Y locatie Rol'])
        df_g['Lat_DP'], df_g['Long_DP'] = from_rd(df_g['X locatie DP'],
                                                  df_g['Y locatie DP'])
        df_g['Size'] = 7
        df_g['Size_DP'] = 14

    count_R = df['RedenNA'].value_counts()
    count_R['R_geen'] = len(df) - sum([el for el in count_R])

    return bar, stats, df_g, count_R


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
