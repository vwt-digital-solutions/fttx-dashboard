# import flask
# import io
import config
import os
# import datetime as dt
import pandas as pd
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
                children=bar_projects(0),
                id="main_graphs0",
                className="container-display",
            ),
            html.Div(
                children=bar_projects(1),
                id="main_graphs0",
                className="container-display",
            ),
            html.Div(
                children=bar_projects(2),
                id="main_graphs0",
                className="container-display",
            ),
            html.Div(
                children=bar_projects(3),
                id="main_graphs0",
                className="container-display",
            ),
            html.Div(
                [
                    html.Div(
                            [dcc.Graph(id="Bar_1")],
                            className="pretty_container column",
                    ),
                    html.Div(
                            [dcc.Graph(id="Bar_2")],
                            className="pretty_container column",
                    ),
                    html.Div(
                            [dcc.Graph(id="Bar_3")],
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
    df_l = data_from_DB()
    list_div = []

    for i, pname in enumerate(df_l.keys()):
        if (i >= s * 5) & (i < (s + 1) * 5):
            fig = go.Figure()
            nb = len(df_l[pname])

            if 2 in df_l[pname]['Opleverstatus'].unique():
                perc_complete = df_l[pname]['Opleverstatus'].value_counts()[2]/nb*100
            else:
                perc_complete = 0

            if 'R0' in df_l[pname]['RedenNA'].unique():
                perc_fout = (df_l[pname]['RedenNA'].value_counts().sum() - df_l[pname]['RedenNA'].value_counts()['R0'])/nb*100
            elif 'R00' in df_l[pname]['RedenNA'].unique():
                perc_fout = (df_l[pname]['RedenNA'].value_counts().sum() - df_l[pname]['RedenNA'].value_counts()['R00'])/nb*100
            else:
                perc_fout = df_l[pname]['RedenNA'].value_counts().sum()/nb * 100

            # x_dloc = int(i / 4) * 0.21
            # y_dloc = (i % 4 * 0.22)
            fig.add_trace(go.Indicator(
                mode="gauge",
                value=perc_complete,
                # align="right",
                # delta={'reference': 100},
                # domain={'x': [0.07 + x_dloc, 0.16 + x_dloc], 'y': [0.15 + y_dloc, 0.20 + y_dloc]},
                domain={'x': [0.2, 1], 'y': [0.5, 0.8]},
                title={'text': '<b>' + pname[0:4] + '</b>', 'font': {'size': 12}},
                gauge={'shape': "bullet",
                       'axis': {'range': [None, 100]},
                       'threshold': {'line': {'color': "black", 'width': 2},
                                     'thickness': 0.75,
                                     'value': 100},
                       'steps': [{'range': [0, round(perc_fout)], 'color': "cyan"},
                                 {'range': [round(perc_fout), 100], 'color': "royalblue"}
                                 ],
                       'bar': {'color': "darkblue"}
                       }))

            fig.update_layout(height=50,
                              width=320,
                              margin={'t': 0, 'b': 0, 'l': 0})
            div = [html.Div(
                            [dcc.Graph(id="Bar_all",
                                       figure=fig)
                             ],
                            )]
            list_div += div

    if s == 3:
        for i in range(len(list_div), 5):
            div = [html.Div(className="pretty_container column")]
            list_div += div

    return list_div


# Globale grafieken
@app.callback(
    [Output("Bar_1", "figure"),
     Output("Bar_2", "figure"),
     Output("Bar_3", "figure"),
     Output("status_table_ext", "children"),
     Output("status_table_ext", "hidden"),
     Output("geo_plot", "figure"),
     ],
    [Input("Bar_all", 'clickData'),
     Input("Bar_1", 'clickData'),
     Input("Bar_2", 'clickData'),
     Input("Bar_3", 'clickData'),
     ]
)
def make_barplot(filter_selectie, cell_b1, cell_b2, cell_b3):
    print(filter_selectie)
    if filter_selectie is None:
        raise PreventUpdate
    df_l = data_from_DB()
    df = df_l[filter_selectie]
    hidden = True

    if cell_b1 is not None:
        hidden = False
        print(cell_b1['points'][0]['x'])
        print(int(cell_b1['points'][0]['curveNumber']))
        pt = abs(int(cell_b1['points'][0]['curveNumber'])-1)
        df = df[df['schouwAkkoord'] == pt]
    if cell_b2 is not None:
        hidden = False
        print(cell_b2['points'][0]['x'])
        print(int(cell_b2['points'][0]['curveNumber']))
        px = cell_b2['points'][0]['x']
        pt = abs(int(cell_b2['points'][0]['curveNumber'])-1)
        if px == 'BIS-civiel':
            df = df[df['Status civiel'] == pt]
        if px == 'BIS-lasAP':
            df = df[df['LaswerkAPGereed'] == pt]
        if px == 'BIS-lasDP':
            df = df[df['LaswerkDPGereed'] == pt]
    if cell_b3 is not None:
        hidden = False
        print(cell_b3['points'][0]['x'])
        print(int(cell_b3['points'][0]['curveNumber']))
        pt = abs(int(cell_b3['points'][0]['curveNumber'])-1)
        if pt == 0:
            df = df[df['Opleverstatus'] != 2]
        if pt == 1:
            df = df[df['Opleverstatus'] == 2]

    if df.empty:
        raise PreventUpdate
    bar, bar2, bar3, stats, geo_plot, df_table = generate_graph(df)
    return [bar, bar2, bar3, df_table, hidden, geo_plot]


# @app.callback(
#     [Output("geo_plot", "figure"),
#      ],
#     [Input("checklist_filters", 'value'),
#      Input("Bar_1", 'clickData'),
#      ]
# )
# def make_geoplot(filter_selectie, clickData):
#     if filter_selectie is None:
#         raise PreventUpdate
#     df_l = data_from_DB()
#     df = df_l[filter_selectie]
#     # if clickData is not None:
#     #     df = df[(df['Opleverstatus'] == int(clickData['points'][0]['x'][1:])) &
#     #             (df['schouwAkkoord'] == int(clickData['points'][0]['curveNumber']))]
#     if df.empty:
#         raise PreventUpdate
#     bar, bar2, bar3, stats, geo_plot, df_table = generate_graph(df)
#     return [geo_plot]


# # DOWNLOAD FUNCTIES
# @app.callback(
#     [Output('download-link', 'href'),
#      #  Output('download-link1', 'href'),
#      #  Output('download-link2', 'href'),
#      ],
#     [Input("checklist_filters", 'value'),
#      #  Input('pie_graph', 'clickData'),
#      ]
# )
# def update_link(filter_selectie):
#     if filter_selectie is None:
#         raise PreventUpdate
#     # if category is None:
#     #     cat = config.beschrijving_cat[0]
#     # else:
#     #     cat = category.get('points')[0].get('label')

#     return ['/download_excel?filters={}'.format(filter_selectie),
#             # '/download_excel1?filters={}'.format(filter_selectie),
#             # '/download_excel2?filters={}'.format(filter_selectie)
#             ]

# # download categorie
# @app.server.route('/download_excel')
# def download_excel():
#     # category = flask.request.args.get('categorie')
#     filter_selectie = flask.request.args.get('filters')
#     df = data_from_DB(filter_selectie)
#     # version_r = max(df['Datum_WF'].dropna().sum()).replace('-', '_')
#     df_table = df

#     # Convert df to excel
#     strIO = io.BytesIO()
#     excel_writer = pd.ExcelWriter(strIO, engine="xlsxwriter")
#     df_table.to_excel(excel_writer, sheet_name="sheet1", index=False)
#     excel_writer.save()
#     strIO.getvalue()
#     strIO.seek(0)

#     # Name download file
#     date = dt.datetime.now().strftime('%d-%m-%Y')
#     filename = "Info_project_filters_{}_{}['x']lsx".format(filter_selectie, date)
#     return send_file(strIO,
#                      attachment_filename=filename,
#                      as_attachment=True)


# HELPER FUNCTIES
@cache.memoize()
def data_from_DB():
    fn = os.listdir(config.path_to_files)
    df_l = {}
    for i, p in enumerate(fn):
        df_l[p[:-13]] = pd.read_csv(config.path_to_files + fn[i], sep=';', encoding='latin-1', low_memory=False)

    return df_l


def generate_graph(df):

    bar, stats, df_g = processed_data(df)

    if bar is not None:
        # info_l = dict(
        #     # R0='Geplande aansluiting',
        #     R1='Geen toestemming bewoner',
        #     R2='Geen toestemming VVE / WOCO',
        #     R3='Bewoner na 3 pogingen niet thuis',
        #     # R4='Nieuwbouw (woning nog niet gereed)',
        #     R5='Hoogbouw obstructie (blokkeert andere bewoners)',
        #     R6='Hoogbouw obstructie (wordt geblokkeerd door andere bewoners)',
        #     R7='Technische obstructie',
        #     R8='Meterkast voldoet niet aan eisen',
        #     # R9='Pand staat leeg',
        #     R10='Geen graafvergunning',
        #     # R11='Aansluitkosten boven normbedrag niet gedekt',
        #     # R12='Buiten het uitrolgebied',
        #     # R13='Glasnetwerk van een andere operator',
        #     # R14='Geen vezelcapaciteit',
        #     # R15='Geen woning',
        #     # R16='Sloopwoning (niet voorbereid)',
        #     # R17='Complex met 1 aansluiting op ander adres',
        #     # R18='Klant niet bereikbaar',
        #     # R19='Bewoner niet thuis, wordt opnieuw ingepland',
        #     # R20='Uitrol na vraagbundeling, klant neemt geen dienst',
        #     # R21='Wordt niet binnen dit project aangesloten',
        #     # R22='Vorst, niet planbaar'
        #     )
        labels = {}
        labels['Schouwen'] = ['Schouwen']
        labels['BIS'] = ['BIS-civiel', 'BIS-lasAP', 'BIS-lasDP']
        labels['HAS'] = ['HAS']

        bar1 = go.Bar(x=labels['Schouwen'],
                      y=bar['Schouwen1'],
                      #   name='SchouwAkkoord',
                      marker=go.bar.Marker(color='rgb(0, 255, 0)'))
        bar2 = go.Bar(x=labels['Schouwen'],
                      y=bar['Schouwen0'],
                      #   name='geen SchouwAkkoord',
                      marker=go.bar.Marker(color='rgb(255, 0, 0)'))
        barc = go.Figure(data=[bar1, bar2],
                         layout=go.Layout(barmode='stack',
                                          clickmode='event+select',
                                          showlegend=False))

        bar1 = go.Bar(x=labels['BIS'],
                      y=bar['BIS-civiel1'] + bar['BIS-lasAP1'] + bar['BIS-lasDP1'],
                      #   name='SchouwAkkoord',
                      marker=go.bar.Marker(color='rgb(0, 255, 0)'))
        bar2 = go.Bar(x=labels['BIS'],
                      y=bar['BIS-civiel0'] + bar['BIS-lasAP0'] + bar['BIS-lasDP0'],
                      #   name='geen SchouwAkkoord',
                      marker=go.bar.Marker(color='rgb(255, 0, 0)'))
        barc2 = go.Figure(data=[bar1, bar2],
                          layout=go.Layout(barmode='stack',
                                           clickmode='event+select',
                                           showlegend=False))

        bar1 = go.Bar(x=labels['HAS'],
                      y=bar['HAS1'],
                      #   name='SchouwAkkoord',
                      marker=go.bar.Marker(color='rgb(0, 255, 0)'))
        bar2 = go.Bar(x=labels['HAS'],
                      y=bar['HAS0'],
                      #   name='geen SchouwAkkoord',
                      marker=go.bar.Marker(color='rgb(255, 0, 0)'))
        barc3 = go.Figure(data=[bar1, bar2],
                          layout=go.Layout(barmode='stack',
                                           clickmode='event+select',
                                           showlegend=False))

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
            title="Woningen (groen = afgerond) & DPs (blauw)",
            mapbox=dict(
                accesstoken=mapbox_access_token,
                style="light",
                center=dict(lon=df_g['Long'].mean(), lat=df_g['Lat'].mean()),
                zoom=13,
            ),
        )

        geo_plot = {'data': map_data, 'layout': map_layout}

    return barc, barc2, barc3, stats, geo_plot, df_table


def processed_data(df):
    # bar chart

    bar = {}
    bar['Schouwen0'] = [len(df[df['schouwAkkoord'] == 0])]
    bar['Schouwen1'] = [len(df[df['schouwAkkoord'] == 1])]
    bar['BIS-civiel0'] = [len(df[df['Status civiel'] == 0])]
    bar['BIS-civiel1'] = [len(df[df['Status civiel'] == 1])]
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

    return bar, stats, df_g


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
