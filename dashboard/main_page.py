# import flask
# import io
import config
import os
# import datetime as dt
import pandas as pd
import numpy as np
import dash_core_components as dcc
import plotly.graph_objs as go
# import dash_daq as daq
# import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_table
# from flask import send_file
# from google.cloud import firestore
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from elements import table_styles
from app import app, cache

# layout graphs
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
                                src=app.get_asset_url("vqd.png"),
                                id="vqd-image",
                                style={
                                    "height": "100px",
                                    "width": "auto",
                                    "margin-bottom": "25px",
                                },
                            )
                        ],
                        className="one-third column",
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H3(
                                        "Status projecten FttX",
                                        style={"margin-bottom": "0px"},
                                    ),
                                    # html.P(),
                                    # html.P("(Laatste update: 05-02-2020)")
                                ],
                                style={"margin-left": "-120px"},
                            )
                        ],
                        className="one-half column",
                        id="title",
                    ),
                ],
                id="header",
                className="row",
                style={"margin-bottom": "25px"},
            ),
            html.Div(
                [
                    html.Div(
                            children=bar_projects(1),
                            className="pretty_container column",
                    ),
                    html.Div(
                            children=bar_projects(0),
                            className="pretty_container column",
                    ),
                ],
                id="main_graphs0",
                className="container-display",
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.H3("Inzoom project:"),
                        ],
                        style={"margin-right": "140px"},
                        # className="pretty_container column",
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
                            className="pretty_container column",
                    ),
                    html.Div(
                            [dcc.Graph(id="Bar_1")],
                            className="pretty_container column",
                    ),
                ],
                id="main_graphs",
                className="container-display",
            ),
            html.Div(
                [
                    html.Div(
                            [dcc.Graph(id="graph_targets")],
                            className="pretty_container column",
                    ),
                    html.Div(
                            [dcc.Graph(id="count_R")],
                            className="pretty_container column",
                    ),
                    html.Div(
                            [dcc.Graph(id="geo_plot")],
                            className="pretty_container column",
                    ),
                ],
                id="main_graphs",
                className="container-display",
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.H3(
                                        "Verdere details:",
                                        style={"margin-bottom": "0px"},
                                    ),
                                ],
                                style={"margin-left": "-120px"},
                            )
                        ],
                        className="one-half column",
                        id="title",
                    ),
                ],
                id="header",
                className="row",
                style={"margin-bottom": "25px"},
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
    df_l, t_s = data_from_DB()
    perc_complete = []
    # perc_fout = []
    pnames = []

    rc1, rc2, rc1_mean, rc2_mean, tot_l, af_l, pnames, df_s_l, x_e_l, y_e_l, x_d, y_cum = speed_projects(df_l, t_s)

    perc_complete = [(el1/el2)*100 for el1, el2 in zip(af_l, tot_l)]

    rc = []
    for i, el in enumerate(rc1):
        if rc2[i] == 0:
            rc += [-rc1[i]]
        else:
            rc += [-rc2[i]]

    # color = [1 if el1 / el2 < 1 else 0 for el1, el2 in zip(perc_fout, perc_complete)]
    # colorscale = [[0, 'red'], [0.5, 'gray'], [1.0, 'green']]

    if s == 0:
        fig = [dcc.Graph(id='project_performance',
                         figure={'data': [
                                          {'x': [0, 80, 80, 100, 100, 0],
                                           'y': [-rc1_mean*0.75, -rc1_mean*0.75,
                                                 -rc2_mean*0.75,  -rc2_mean*0.75,
                                                 -rc1_mean*1.75, -rc1_mean*1.75
                                                 ],
                                           'name': 'Trace 2',
                                           'mode': 'lines',
                                           'fill': 'toself',
                                           'line': {'color': 'rgb(0, 200, 0)'}
                                           },
                                          {'x': perc_complete,
                                           #    'y': perc_fout,
                                           'y': rc,
                                           'text': pnames,
                                           'name': 'Trace 1',
                                           'mode': 'markers',
                                           'marker': {'size': 15}
                                           },
                                          ],
                                 'layout': {'clickmode': 'event+select',
                                            'xaxis': {'title': 'huizen afgerond [%]'},
                                            'yaxis': {'title': 'snelheid [woningen / dag]'},
                                            'showlegend': False,
                                            'title': {'text': 'Klik op een project voor meer informatie! <br> [projecten binnen het groene vlak verlopen volgens verwachting]'},
                                            }
                                 }
                         )]

    if s == 1:
        fig = [dcc.Graph(id="graph_progT",
                         figure={'data': [{'x': list(x_d[0:1000]),
                                           'y': list(y_cum[0:1000]),
                                           'mode': 'lines'
                                           },
                                          ],
                                 'layout': {
                                            'xaxis': {'title': 'Opleverdatum [dag]',
                                                      'range': [min(t_s.values()),
                                                                '2022-01-01']},
                                            'yaxis': {'title': 'Aantal huizen nog aan te sluiten',
                                                               'range': [0, 130000]},
                                            'showlegend': False,
                                            'title': {'text': 'Prognose werkvoorraad FttX:'},
                                            }
                                 }
                         )
               ]

    if s == 2:
        filters = []
        for el in pnames:
            filters += [{'label': el, 'value': el}]
        fig = filters

    return fig


# Globale grafieken
@app.callback(
    [Output("Bar_1", "figure"),
     Output("status_table_ext", "children"),
     Output("status_table_ext", "hidden"),
     Output("geo_plot", "figure"),
     Output("count_R", "figure"),
     Output("aggregate_data", 'data'),
     Output("aggregate_data2", 'data'),
     Output("graph_prog", 'figure'),
     Output("graph_targets", 'figure'),
     ],
    [Input("project_performance", 'clickData'),
     Input('project-dropdown', 'value'),
     Input("Bar_1", 'clickData'),
     Input("count_R", 'clickData'),
     ],
    [State("aggregate_data", 'data'),
     State("aggregate_data2", 'data'),
     ]
)
def make_barplot(filter_selectie, drop_selectie, cell_b1, cell_bR, mask_all, filter_a):
    if (filter_selectie is None) & (drop_selectie is None):
        raise PreventUpdate
    if drop_selectie is not None:
        filter_selectie = drop_selectie
    else:
        filter_selectie = filter_selectie['points'][0]['text']

    df_l, t_s = data_from_DB()
    df = df_l[filter_selectie]
    hidden = True

    if cell_b1 is None:
        mask_all = None
    elif filter_selectie == filter_a:
        hidden = False
        pt_f = cell_b1['points'][0]['x']
        if cell_b1['points'][0]['curveNumber'] == 0:
            pt_c = 'LB1'
        if cell_b1['points'][0]['curveNumber'] == 1:
            pt_c = 'LB1HP'
        if cell_b1['points'][0]['curveNumber'] == 2:
            pt_c = 'LB0'
        if cell_b1['points'][0]['curveNumber'] == 3:
            pt_c = 'HB1'
        if cell_b1['points'][0]['curveNumber'] == 4:
            pt_c = 'HB1HP'
        if cell_b1['points'][0]['curveNumber'] == 5:
            pt_c = 'HB0'

        bar, _, _, _ = processed_data(df)

        mask = bar[pt_f + pt_c + '-mask']

        print(cell_b1)
        print(pt_f)
        print(pt_c)

        if mask_all is None:
            mask_all = mask
        else:
            mask_all = mask_all & mask

        df = df[mask_all]

    if df.empty:
        raise PreventUpdate
    rc1, rc2, rc1_mean, rc2_mean, tot_l, af_l, pnames, df_s_l, \
        x_e_l, y_e_l, x_d, y_cum = speed_projects(df_l, t_s)
    bar, stats, geo_plot, df_table, bar_R, fig_prog, \
        fig_targets = generate_graph(
            df, x_e_l, y_e_l, df_s_l, filter_selectie,
            x_d, y_cum, t_s)
    return [bar, df_table, hidden, geo_plot, bar_R, mask_all,
            filter_selectie, fig_prog, fig_targets]


# HELPER FUNCTIES
@cache.memoize()
def data_from_DB():
    fn = os.listdir(config.path_to_files)
    df_l = {}
    t_s = {}
    for i, p in enumerate(fn):
        df_l[p[:-13]] = pd.read_csv(
            config.path_to_files + fn[i], sep=';',
            encoding='latin-1', low_memory=False)
        t_min = pd.to_datetime(
            df_l[p[:-13]]['Opleverdatum'], format='%d-%m-%Y').min()
        if not pd.isnull(t_min):
            t_s[p[:-13]] = t_min
    del df_l['LCM project']
    del t_s['LCM project']

    return df_l, t_s


def generate_graph(df, x_e_l, y_e_l, df_s_l, filter_selectie, x_d, y_cum, t_s):

    bar, stats, df_g, count_R = processed_data(df)

    if bar is not None:
        reden_l = dict(
            R0='Geplande aansluiting',
            R1='Geen toestemming bewoner',
            R2='Geen toestemming VVE / WOCO',
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
                       name='LB1HC',
                       marker=go.bar.Marker(color='rgb(0, 200, 0)'))
        bar1b = go.Bar(x=labels['OHW'],
                       y=[0] +
                       [0] +
                       [0] +
                       [0] +
                       bar['HASLB1HP'],
                       name='LB1HP',
                       marker=go.bar.Marker(color='rgb(0, 230, 0)'))
        bar1c = go.Bar(x=labels['OHW'],
                       y=bar['SchouwenLB0'] +
                       bar['BISLB0'] +
                       bar['Montage-lasAPLB0'] +
                       bar['Montage-lasDPLB0'] +
                       bar['HASLB0'],
                       name='LB0',
                       marker=go.bar.Marker(color='rgb(200, 0, 0)'))
        bar1d = go.Bar(x=labels['OHW'],
                       y=bar['SchouwenHB1'] +
                       bar['BISHB1'] +
                       bar['Montage-lasAPHB1'] +
                       bar['Montage-lasDPHB1'] +
                       bar['HASHB1'],
                       name='HB1HC',
                       marker=go.bar.Marker(color='rgb(0, 255, 0)'))
        bar1e = go.Bar(x=labels['OHW'],
                       y=[0] +
                       [0] +
                       [0] +
                       [0] +
                       bar['HASHB1HP'],
                       name='HB1HP',
                       marker=go.bar.Marker(color='rgb(0, 230, 0)'))
        bar1f = go.Bar(x=labels['OHW'],
                       y=bar['SchouwenHB0'] +
                       bar['BISHB0'] +
                       bar['Montage-lasAPHB0'] +
                       bar['Montage-lasDPHB0'] +
                       bar['HASHB0'],
                       name='HB0',
                       marker=go.bar.Marker(color='rgb(255, 0, 0)'))

        barc = go.Figure(data=[bar1a, bar1b, bar1c, bar1d, bar1e, bar1f],
                         layout=go.Layout(barmode='stack',
                                          clickmode='event+select',
                                          showlegend=True,
                                          title={'text': 'OHW per projectfase voor LB en HB [rood]:',
                                                 'x': 0.5}
                                          ))

        df_t = df[['Sleutel', 'Opleverdatum',
                   'Opleverstatus', 'Internestatus',
                   'RedenNA', 'schouwAkkoord',
                   'HasApp_Status', 'Toelichting status']].copy()
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

        bar1 = go.Bar(x=count_R.index.to_list(),
                      y=count_R.to_list(),
                      text=[el + ': ' + reden_l[el]
                            for el in count_R.index.to_list()],
                      marker=go.bar.Marker(color='rgb(255, 0, 0)'))
        bar_R = go.Figure(data=[bar1],
                          layout=go.Layout(barmode='stack',
                                           clickmode='event+select',
                                           showlegend=False,
                                           #    yaxis={'title': {'text': '%'}},
                                           ))
        fig_prog = {'data': [{'x': list(x_e_l[filter_selectie]),
                              'y': list(y_e_l[filter_selectie]),
                              'mode': 'lines'
                              },
                             {'x': df_s_l[filter_selectie].index.to_list(),
                              'y': df_s_l[
                                   filter_selectie]['Sleutel'].to_list(),
                              'mode': 'markers'
                              }
                             ],
                    'layout': {
                               'xaxis': {'title': 'opleverdagen [dag]',
                                         'range': [0, 3*365]},
                               'yaxis': {'title': 'Totaal afgerond [%]',
                                         'range': [0, 110]},
                               'title': {'text': 'Snelheid project & prognose afronding:'},
                               'showlegend': False,
                               }
                    }

        dat_opg = pd.to_datetime(df[(~df['HASdatum'].isna()) &
                                    (~df['Opleverdatum'].isna())][
                                        'Opleverdatum'], format='%d-%m-%Y')
        dat_HAS = pd.to_datetime(df[(~df['HASdatum'].isna()) &
                                    (~df['Opleverdatum'].isna())]['HASdatum'],
                                 format='%d-%m-%Y')
        dat_diff = (((dat_HAS - dat_opg).dt.days + 7) / 7).astype(int)

        fig_targets = {'data': [{'x': dat_diff.to_list(),
                                 'type': 'histogram'
                                 },
                                ],
                       'layout': {
                                 'xaxis': {'title': 'week',
                                 'range': [0, 5]},
                                 'showlegend': False,
                                 'title': {'text': 'Verschil tussen HASdatum en Opleverdatum: <br> [target is max 1 week]'},
                               }
                       }

    if df_g is not None:
        token_1 = 'pk.eyJ1IjoiamFja2x1byIsImEiOiJjajNlcnh3MzEwMHZtMzNueGw'
        token_2 = '3NWw5ZXF5In0.fk8k06T96Ml9CLGgKmk81w'
        mapbox_access_token = token_1 + token_2
        normalized_size = df_g['Size'].to_list() + df_g['Size_DP'].to_list()

        map_data = [
            go.Scattermapbox(
                lat=df_g['Lat'].to_list() + df_g['Lat_DP'].to_list(),
                lon=df_g['Long'].to_list() + df_g['Long_DP'].to_list(),
                mode='markers',
                marker=dict(
                    cmax=50,
                    cmin=0,
                    color=df_g['clr'].to_list() + df_g['clr-DP'].to_list(),
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
            margin=dict(r=30, b=20, t=40),
            hovermode="closest",
            plot_bgcolor="#F9F9F9",
            paper_bgcolor="#F9F9F9",
            legend=dict(font=dict(size=10), orientation="h"),
            title="Woningen (klein) & DPs (groot) [groen = afgerond]",
            mapbox=dict(
                accesstoken=mapbox_access_token,
                style="light",
                center=dict(lon=df_g['Long'].mean(), lat=df_g['Lat'].mean()),
                zoom=13,
            ),
        )

        geo_plot = {'data': map_data, 'layout': map_layout}

    return barc, stats, geo_plot, df_table, bar_R, fig_prog, \
        fig_targets


def processed_data(df):
    # bar chart

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

    # stats
    stats = {'0': str(round(len(df))),
             '1': str(round(0)),
             '2': str(round(0))}

    # geoplot
    # df_g = df.groupby('Sleutel').agg(
    #     {
    #         'Sleutel': 'count',
    #         'Opleverstatus': lambda x: sum(x == 2),
    #         'X locatie Rol': 'first',
    #         'Y locatie Rol': 'first',
    #         'X locatie DP': 'first',
    #         'Y locatie DP': 'first'
    #     }
    # )
    # df_g.reset_index(inplace=True)
    df_g = df.copy()
    df_g['clr'] = 0
    df_g.loc[df_g['Opleverstatus'] == 2, ('clr')] = 50
    df_g['clr-DP'] = 0
    df_g.loc[df_g['LaswerkDPGereed'] == 1, ('clr-DP')] = 50
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
    # if 'R0'in count_R:
    #     del count_R['R0']
    # if 'R00'in count_R:
    #     del count_R['R00']
    count_R['R_geen'] = len(df) - sum([el for el in count_R])
    print(count_R)
    # for key in count_R.keys():
    #     count_R[key] = count_R[key] / len(df) * 100

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


def speed_projects(df_l, t_s):
    # df_ss = []
    rc1 = []
    rc2 = []
    count_rc2 = 0
    cutoff = 15
    df_s_l = {}
    b1 = []
    b2 = []
    tot_l = []
    af_l = []
    t_shift = []
    pnames = []
    for i, key in enumerate(df_l):
        df_test = df_l[key]
        tot = len(df_test)
        # df_test = df_test[df_test['Opleverstatus'] == 2]
        df_test = df_test[~df_test['Opleverdatum'].isna()]
        af = len(df_test)
        if not df_test.empty:
            df_s = df_test.groupby(['Opleverdatum']).agg({'Sleutel': 'count'})
            df_s.index = pd.to_datetime(df_s.index, format='%d-%m-%Y')
            df_s = df_s.sort_index()
            df_s = df_s[df_s.index < '2020-02-26']
            df_s['Sleutel'] = df_s['Sleutel'].cumsum()
            df_s['Sleutel'] = 100 - df_s['Sleutel'] / tot * 100
            # df_s = df_s / 100 * tot
            t_sh = (df_s.index.min() - min(t_s.values())).days
            df_s.index = (df_s.index - df_s.index[0]).days + t_sh

            df_s_rc1 = df_s[df_s['Sleutel'] > cutoff].copy()
            df_s_rc2 = df_s[df_s['Sleutel'] <= cutoff].copy()

            if not df_s_rc1.empty:
                z1 = np.polyfit(df_s_rc1.index, df_s_rc1.Sleutel, 1)
                # p1 = np.poly1d(z1)
                rc1 += [z1[0] / 100 * tot]   # huizen/d
                b1 += [z1[1]]
            else:
                rc1 += [0]
                b1 += [0]
            if not df_s_rc2.empty:
                z2 = np.polyfit(df_s_rc2.index, df_s_rc2.Sleutel, 1)
                # p2 = np.poly1d(z2)
                rc2 += [z2[0] / 100 * tot]   # huizen/d
                count_rc2 += 1
                b2 += [z2[1]]
            else:
                rc2 += [0]
                b2 += [0]

            df_s_l[key] = df_s
            tot_l += [tot]
            af_l += [af]
            t_shift += [t_sh]
            pnames += [key]

    rc1[-3:-2] = [0]  # uitzondering voor "Nijmegen Biezen-Wolfskuil-Hatert
    rc1_mean = sum(rc1) / len(rc1)
    rc2_mean = sum(rc2) / count_rc2
    b2_mean = sum(b2) / count_rc2

    # prognose
    ts = 0
    te = 12000
    # df_y = pd.DataFrame()
    x_e_l = {}
    y_e_l = {}

    for i, key in enumerate(df_s_l):

        x_e = np.array(list(range(ts, te + 1)))
        x_e_l[key] = x_e
        a1 = (rc1[i] / tot_l[i] * 100)
        y_e1 = b1[i] + a1 * x_e
        a2 = (rc2[i] / tot_l[i] * 100)
        b2i = b2[i]
        if a2 == 0:
            a2 = (rc2_mean / tot_l[i] * 100)
            b2i = b2_mean
        y_e2 = b2i + a2 * x_e

        y_ed = y_e1 - y_e2
        y_e = y_e1
        y_e[y_ed < 0] = y_e2[y_ed < 0]
        y_e[x_e < df_s_l[key].index.min()] = 0
        y_e_l[key] = y_e

        if i == 0:
            y_cum = y_e / 100 * tot_l[i]
        else:
            y_cum += y_e / 100 * tot_l[i]

    x_d = pd.date_range(min(t_s.values()), periods=te + 1, freq='D')

    return rc1, rc2, rc1_mean, rc2_mean, tot_l, af_l, pnames, \
        df_s_l, x_e_l, y_e_l, x_d, y_cum
