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
from dash.dependencies import Input, Output
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
                      data={'0': '0', '1': '0', '2': '0'}),
            dcc.Store(id="aggregate_data2",
                      data={'0': '0', '1': '0', '2': '0'}),
            # html.Div(
            #     [
            #         html.Div(
            #             [
            #                 html.Img(
            #                     src=app.get_asset_url("vqd.png"),
            #                     id="vqd-image",
            #                     style={
            #                         "height": "100px",
            #                         "width": "auto",
            #                         "margin-bottom": "25px",
            #                     },
            #                 )
            #             ],
            #             className="one-third column",
            #         ),
            #         html.Div(
            #             [
            #                 # html.Div(
            #                 #     [
            #                 #         html.H3(
            #                 #             "Analyse OHW VWT FTTx",
            #                 #             style={"margin-bottom": "0px"},
            #                 #         ),
            #                 #         # html.P(),
            #                 #         # html.P("(Laatste update: 05-02-2020)")
            #                 #     ],
            #                 #     style={"margin-left": "-120px"},
            #                 # )
            #             ],
            #             className="one-half column",
            #             id="title",
            #         ),
            #     ],
            #     id="header",
            #     className="row",
            #     style={"margin-bottom": "25px"},
            # ),
            html.Div(
                [
                    html.Div(
                            children=bar_projects(),
                            className="pretty_container column",
                    ),
                    html.Div(
                            [dcc.Graph(id="geo_plot")],
                            className="pretty_container column",
                    ),
                ],
                id="main_graphs0",
                className="container-display",
            ),
            html.Div(
                [
                    html.Div(
                            [dcc.Graph(id="Bar_1")],
                            className="pretty_container column",
                    ),
                    # html.Div(
                    #         [dcc.Graph(id="Bar_2")],
                    #         className="pretty_container column",
                    # ),
                    # html.Div(
                    #         [dcc.Graph(id="Bar_3")],
                    #         className="pretty_container column",
                    # ),
                    html.Div(
                            [dcc.Graph(id="count_R")],
                            className="pretty_container column",
                    ),
                ],
                id="main_graphs",
                className="container-display",
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


def bar_projects():
    df_l, t_s = data_from_DB()
    perc_complete = []
    perc_fout = []
    pnames = []

    for i, pname in enumerate(df_l.keys()):
        nb = len(df_l[pname])
        pnames += [pname]

        if 2 in df_l[pname]['Opleverstatus'].unique():
            perc_complete += [df_l[pname]['Opleverstatus'].value_counts()[2]/nb*100]
        else:
            perc_complete += [0]

        if 'R0' in df_l[pname]['RedenNA'].unique():
            perc_fout += [(df_l[pname]['RedenNA'].value_counts().sum() - df_l[pname]['RedenNA'].value_counts()['R0'])/nb*100]
        elif 'R00' in df_l[pname]['RedenNA'].unique():
            perc_fout += [(df_l[pname]['RedenNA'].value_counts().sum() - df_l[pname]['RedenNA'].value_counts()['R00'])/nb*100]
        else:
            perc_fout += [df_l[pname]['RedenNA'].value_counts().sum()/nb * 100]

    rc1, rc2, rc1_mean, rc2_mean, tot_l = speed_projects(df_l, t_s)
    rc = []
    for i, el in enumerate(rc1):
        if rc2[i] == 0:
            rc += [-rc1[i]]
        else:
            rc += [-rc2[i]]

    # color = [1 if el1 / el2 < 1 else 0 for el1, el2 in zip(perc_fout, perc_complete)]
    # colorscale = [[0, 'red'], [0.5, 'gray'], [1.0, 'green']]
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
                                         'marker': {'size': 15,
                                                    #   'color': color,
                                                    #   'colorscale': colorscale,
                                                    }
                                         },
                                      ],
                             'layout': {'clickmode': 'event+select',
                                        'xaxis': {'title': 'huizen afgerond (%)'},
                                        'yaxis': {'title': 'snelheid [woningen / dag]'},
                                        }
                             }
                     )]

    return fig


# Globale grafieken
@app.callback(
    [Output("Bar_1", "figure"),
     Output("status_table_ext", "children"),
     Output("status_table_ext", "hidden"),
     Output("geo_plot", "figure"),
     Output("count_R", "figure"),
     ],
    [Input("project_performance", 'clickData'),
     Input("Bar_1", 'clickData'),
     Input("count_R", 'clickData'),
     ]
)
def make_barplot(filter_selectie, cell_b1, cell_bR):
    if filter_selectie is None:
        raise PreventUpdate
    df_l, _ = data_from_DB()
    df = df_l[filter_selectie['points'][0]['text']]
    hidden = True

    if cell_b1 is not None:
        hidden = False
        pt_f = cell_b1['points'][0]['x']
        pt_c = cell_b1['points'][0]['curveNumber']
        print(pt_f)
        print(pt_c)

        if pt_f == 'Schouwen-toestemmingcheck (LB/HB)':
            if pt_c == 0:
                mask = (~df['Toestemming'].isna()) & (df['Soort_bouw'] != 'Hoog')
            if pt_c == 1:
                mask = (~df['Toestemming'].isna()) & (df['Soort_bouw'] == 'Hoog')
            if pt_c == 2:
                mask = (df['Toestemming'].isna()) & (df['Soort_bouw'] != 'Hoog')
            if pt_c == 3:
                mask = (df['Toestemming'].isna()) & (df['Soort_bouw'] == 'Hoog')

        if pt_f == 'Schouwen-gereed':
            if pt_c == 0:
                mask = (df['D2D_status'] == 'Bij de weg') & (df['Toestemming'] == 'Ja')
            if pt_c == 2:
                mask = df['D2D_status'] == 'Bij de Woning'

        if pt_f == 'BIS-civiel':
            if pt_c == 0:
                mask = df['Status civiel'].astype(str) == '0'
            if pt_c == 1:
                mask = df['Status civiel'].astype(str) == '1'

        if pt_f == 'Montage-lasAP':
            if pt_c == 0:
                mask = df['LaswerkAPGereed'] == 0
            if pt_c == 1:
                mask = df['LaswerkAPGereed'] == 1

        if pt_f == 'Montage-lasDP':
            if pt_c == 0:
                mask = df['LaswerkDPGereed'] == 0
            if pt_c == 1:
                mask = df['LaswerkDPGereed'] == 1

        if pt_f == 'HAS':
            if pt_c == 0:
                mask = df['Opleverstatus'] != 2
            if pt_c == 1:
                mask = df['Opleverstatus'] == 2

        df = df[mask]

    if df.empty:
        raise PreventUpdate
    bar, stats, geo_plot, df_table, bar_R = generate_graph(df)
    return [bar, df_table, hidden, geo_plot, bar_R]


# HELPER FUNCTIES
@cache.memoize()
def data_from_DB():
    fn = os.listdir(config.path_to_files)
    df_l = {}
    t_s = {}
    for i, p in enumerate(fn):
        df_l[p[:-13]] = pd.read_csv(config.path_to_files + fn[i], sep=';', encoding='latin-1', low_memory=False)
        t_min = pd.to_datetime(df_l[p[:-13]]['Opleverdatum'], format='%d-%m-%Y').min()
        if not pd.isnull(t_min):
            t_s[p[:-13]] = t_min
    del df_l['LCM project']
    del t_s['LCM project']

    return df_l, t_s


def generate_graph(df):

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
            R_geen='Geen reden of R0'
            )
        labels = {}
        labels['Schouwen'] = ['Schouwen-toestemmingcheck (LB/HB)', 'Schouwen-gereed']
        labels['BIS'] = ['BIS-civiel', 'Montage-lasAP', 'Montage-lasDP']
        labels['HAS'] = ['HAS']

        bar1a = go.Bar(x=labels['Schouwen'],
                       y=bar['Schouwen-toestemmingLB1'] + bar['Schouwen-afgerond1'],
                       #   name='SchouwAkkoord',
                       marker=go.bar.Marker(color='rgb(0, 200, 0)'))
        bar1b = go.Bar(x=labels['Schouwen'],
                       y=bar['Schouwen-toestemmingHB1'] + [0],
                       #   name='geen SchouwAkkoord',
                       marker=go.bar.Marker(color='rgb(0, 255, 0)'))
        bar1c = go.Bar(x=labels['Schouwen'],
                       y=bar['Schouwen-toestemmingLB0'] + bar['Schouwen-afgerond0'],
                       #   name='geen SchouwAkkoord',
                       marker=go.bar.Marker(color='rgb(200, 0, 0)'))
        bar1d = go.Bar(x=labels['Schouwen'],
                       y=bar['Schouwen-toestemmingHB0'] + [0],
                       #   name='geen SchouwAkkoord',
                       marker=go.bar.Marker(color='rgb(255, 0, 0)'))
        bar2a = go.Bar(x=labels['BIS'],
                       y=bar['BIS-civiel1'] + bar['BIS-lasAP1'] + bar['BIS-lasDP1'],
                       #   name='SchouwAkkoord',
                       marker=go.bar.Marker(color='rgb(0, 200, 0)'))
        bar2b = go.Bar(x=labels['BIS'],
                       y=bar['BIS-civiel0'] + bar['BIS-lasAP0'] + bar['BIS-lasDP0'],
                       #   name='geen SchouwAkkoord',
                       marker=go.bar.Marker(color='rgb(200, 0, 0)'))
        bar3a = go.Bar(x=labels['HAS'],
                       y=bar['HAS1'],
                       #   name='SchouwAkkoord',
                       marker=go.bar.Marker(color='rgb(0, 200, 0)'))
        bar3b = go.Bar(x=labels['HAS'],
                       y=bar['HAS0'],
                       #   name='geen SchouwAkkoord',
                       marker=go.bar.Marker(color='rgb(200, 0, 0)'))
        barc = go.Figure(data=[bar1a, bar1b, bar1c, bar1d, bar2a, bar2b, bar3a, bar3b],
                         layout=go.Layout(barmode='stack',
                                          clickmode='event+select',
                                          showlegend=False,
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
                      #   name=str([el + ': ' + reden_l[el] for el in count_R.index.to_list()]),
                      text=[el + ': ' + reden_l[el] for el in count_R.index.to_list()],
                      marker=go.bar.Marker(color='rgb(255, 0, 0)'))
        bar_R = go.Figure(data=[bar1],
                          layout=go.Layout(barmode='stack',
                                           clickmode='event+select',
                                           showlegend=False,
                                           #    yaxis={'title': {'text': '%'}},
                                           ))

    if df_g is not None:
        mapbox_access_token = "pk.eyJ1IjoiamFja2x1byIsImEiOiJjajNlcnh3MzEwMHZtMzNueGw3NWw5ZXF5In0.fk8k06T96Ml9CLGgKmk81w"
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
                    # colorbar=dict(
                    #     title='Colorbar'
                    # ),
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

    return barc, stats, geo_plot, df_table, bar_R


def processed_data(df):
    # bar chart

    bar = {}
    bar['Schouwen-toestemmingLB0'] = [len(df[(df['Toestemming'].isna()) &
                                             (df['Soort_bouw'] != 'Hoog')])]
    bar['Schouwen-toestemmingLB1'] = [len(df[(~df['Toestemming'].isna()) &
                                             (df['Soort_bouw'] != 'Hoog')])]
    bar['Schouwen-toestemmingHB0'] = [len(df[(df['Toestemming'].isna()) &
                                             (df['Soort_bouw'] == 'Hoog')])]
    bar['Schouwen-toestemmingHB1'] = [len(df[(~df['Toestemming'].isna()) &
                                             (df['Soort_bouw'] == 'Hoog')])]
    bar['Schouwen-afgerond0-ohw'] = [len(df[(df['D2D_status'] == 'Bij de weg') &
                                            (df['Toestemming'] == 'Ja')])]
    bar['Schouwen-afgerond0'] = [len(df[((df['D2D_status'] == 'Bij de weg') &
                                        (df['Toestemming'] != 'Ja')) |
                                        (df['D2D_status'].isna())])]
    bar['Schouwen-afgerond1'] = [len(df[df['D2D_status'] == 'Bij de Woning'])]
    bar['BIS-civiel0'] = [len(df[df['Status civiel'].astype(str) == '0'])]
    bar['BIS-civiel1'] = [len(df[df['Status civiel'].astype(str) == '1'])]
    bar['BIS-lasAP0'] = [len(df[df['LaswerkAPGereed'] == 0])]
    bar['BIS-lasAP1'] = [len(df[df['LaswerkAPGereed'] == 1])]
    bar['BIS-lasDP0'] = [len(df[df['LaswerkDPGereed'] == 0])]
    bar['BIS-lasDP1'] = [len(df[df['LaswerkDPGereed'] == 1])]
    bar['HAS0'] = [len(df[df['Opleverstatus'] != 2])]
    bar['HAS1'] = [len(df[df['Opleverstatus'] == 2])]

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
    df_g['X locatie Rol'] = df_g['X locatie Rol'].str.replace(',', '.').astype(float)
    df_g['Y locatie Rol'] = df_g['Y locatie Rol'].str.replace(',', '.').astype(float)
    df_g['X locatie DP'] = df_g['X locatie DP'].str.replace(',', '.').astype(float)
    df_g['Y locatie DP'] = df_g['Y locatie DP'].str.replace(',', '.').astype(float)
    df_g['Lat'], df_g['Long'] = from_rd(df_g['X locatie Rol'], df_g['Y locatie Rol'])
    df_g['Lat_DP'], df_g['Long_DP'] = from_rd(df_g['X locatie DP'], df_g['Y locatie DP'])
    df_g['Size'] = 7
    df_g['Size_DP'] = 14

    count_R = df['RedenNA'].value_counts()
    if 'R0'in count_R:
        del count_R['R0']
    if 'R00'in count_R:
        del count_R['R00']
    count_R['R_geen'] = len(df) - sum([el for el in count_R])
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
    Kpq = [3235.65389, -32.58297, -0.24750, -0.84978, -0.06550, -0.01709, -0.00738, 0.00530, -0.00039,
           0.00033, -0.00012]

    Lp = [1, 1, 1, 3, 1, 3, 0, 3, 1, 0, 2, 5]
    Lq = [0, 1, 2, 0, 3, 1, 1, 2, 4, 2, 0, 0]
    Lpq = [5260.52916, 105.94684, 2.45656, -0.81885, 0.05594, -0.05607, 0.01199, -0.00256, 0.00128, 0.00022,
           -0.00022, 0.00026]
    # # Coefficients for the conversion from WGS84 to RD
    # Rp = [0, 1, 2, 0, 1, 3, 1, 0, 2]
    # Rq = [1, 1, 1, 3, 0, 1, 3, 2, 3]
    # Rpq = [190094.945, -11832.228, -114.221, -32.391, -0.705, -2.340, -0.608, -0.008, 0.148]

    # Sp = [1, 0, 2, 1, 3, 0, 2, 1, 0, 1]
    # Sq = [0, 2, 0, 2, 0, 1, 2, 1, 4, 4]
    # Spq = [309056.544, 3638.893, 73.077, -157.984, 59.788, 0.433, -6.439, -0.032, 0.092, -0.054]
    """
    Converts RD coordinates into WGS84 coordinates
    """
    dx = 1E-5 * (x - x0)
    dy = 1E-5 * (y - y0)
    latitude = phi0 + sum([v * dx ** Kp[i] * dy ** Kq[i] for i, v in enumerate(Kpq)]) / 3600
    longitude = lam0 + sum([v * dx ** Lp[i] * dy ** Lq[i] for i, v in enumerate(Lpq)]) / 3600

    return latitude, longitude


def speed_projects(df_l, t_s):
    # df_ss = []
    rc1 = []
    rc2 = []
    count_rc2 = 0
    cutoff = 20
    df_s_l = {}
    b1 = []
    b2 = []
    tot_l = []
    t_shift = []
    for i, key in enumerate(df_l):
        df_test = df_l[key]
        tot = len(df_test)
        df_test = df_test[df_test['Opleverstatus'] == 2]
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

            # x_e = df_s.index.to_list() + list(range(df_s.index.max() + 1, 500))
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
            t_shift += [t_sh]

    rc1_mean = sum(rc1) / len(rc1)
    rc2_mean = sum(rc2) / count_rc2
    # b2_mean = sum(b2) / count_rc2

    return rc1, rc2, rc1_mean, rc2_mean, tot_l
