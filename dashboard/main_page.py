import flask
import io
import config
import datetime as dt
import pandas as pd
import dash_core_components as dcc
import plotly.graph_objs as go
import dash_daq as daq
# import dash_bootstrap_components as dbc
import dash_html_components as html
# import dash_table
from flask import send_file
# from google.cloud import firestore
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
# from elements import table_styles
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
                                        "Analyse OHW VWT FTTx",
                                        style={"margin-bottom": "0px"},
                                    ),
                                    # html.H5(
                                    #     "FTTx",
                                    #     style={"margin-top": "0px"}
                                    # ),
                                    html.P(),
                                    html.P("(Laatste update: 05-02-2020)")
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
                        [
                            html.Div(
                                [
                                    # html.P("Filters:"),
                                    dcc.Dropdown(
                                        options=[
                                            {'label': 'Brielle',
                                                'value': 'Brielle'},
                                            {'label': 'Nijmegen',
                                                'value': 'Nijmegen'},
                                            {'label': 'Helvoirt',
                                                'value': 'Helvoirt'},
                                            {'label': 'Dongen',
                                                'value': 'Dongen'},
                                        ],
                                        id='checklist_filters',
                                        value='Brielle',
                                        # multi=True,
                                    ),
                                ],
                                id="filter_container",
                                className="pretty_container_title columns",
                            ),
                            html.Div(
                                [
                                    daq.Gauge(
                                        id='my-gauge-1',
                                        label="Default",
                                        value=6,
                                        size=100,
                                        max=10,
                                    ),
                                ],
                                id="download_container",
                                className="pretty_container_title columns",
                            ),
                            html.Div(
                                [
                                    daq.Gauge(
                                        id='my-gauge-2',
                                        label="Default",
                                        value=6,
                                        size=100,
                                    ),
                                ],
                                id="download_container",
                                className="pretty_container_title columns",
                            ),
                            html.Div(
                                [
                                    daq.Gauge(
                                        id='my-gauge-3',
                                        label="Default",
                                        value=6,
                                        size=100,
                                    ),
                                ],
                                id="download_container",
                                className="pretty_container_title columns",
                            ),
                            html.Div(
                                [
                                    daq.Gauge(
                                        id='my-gauge-4',
                                        label="Default",
                                        value=6,
                                        size=100,
                                    ),
                                ],
                                id="download_container",
                                className="pretty_container_title columns",
                            ),
                        ],
                        id="info-container",
                        className="container-display",
                    ),
                ],
            ),
            # html.Div(
            #     [
            #         html.Div(
            #             [
            #                 html.H5(
            #                     "Totaal overzicht OHW analyse:",
            #                     style={"margin-top": "0px"}
            #                 ),
            #             ],
            #             id='uitleg_1',
            #             className="pretty_container_title columns",
            #         ),
            #     ],
            #     className="container-display",
            # ),
            # html.Div(
            #     [
            #         html.Div(
            #             [
            #                 html.Div(
            #                     [
            #                         html.H6(id="info_globaal_0"),
            #                         html.P("Aantal woningen in project")
            #                     ],
            #                     id="info_globaal_container0",
            #                     className="pretty_container 3 columns",
            #                 ),
            #                 html.Div(
            #                     [
            #                         html.H6(id="info_globaal_1"),
            #                         html.P("-")
            #                     ],
            #                     id="info_globaal_container1",
            #                     className="pretty_container 3 columns",
            #                 ),
            #                 html.Div(
            #                     [
            #                         html.H6(id="info_globaal_2"),
            #                         html.P("-")
            #                     ],
            #                     id="info_globaal_container2",
            #                     className="pretty_container 3 columns",
            #                 ),
            #             ],
            #             id="info-container1",
            #             className="container-display",
            #         ),
            #     ],
            # ),
            html.Div(
                [
                    html.Div(
                            [dcc.Graph(id="OHW_globaal_graph")],
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
            # html.Div(
            #     [
            #         html.Div(
            #             [
            #                 html.H5(
            #                     """
            #                     Categorisering van de projecten met OHW:
            #                     """,
            #                     style={"margin-top": "0px"}
            #                 ),
            #             ],
            #             id='uitleg_2',
            #             className="pretty_container_title columns",
            #         ),
            #     ],
            #     className="container-display",
            # ),
            # html.Div(
            #     [
            #         html.Div(
            #             [
            #                 html.Div(
            #                     [
            #                         html.H6(id="info_bakje_0"),
            #                         html.P("Aantal projecten in categorie")

            #                     ],
            #                     className="pretty_container 3 columns",
            #                 ),
            #                 html.Div(
            #                     [
            #                         html.H6(id="info_bakje_1"),
            #                         html.P("Aantal meter OHW in categorie")
            #                     ],
            #                     className="pretty_container 3 columns",
            #                 ),
            #                 html.Div(
            #                     [
            #                         html.H6(id="info_bakje_2"),
            #                         html.P("""Aantal meter extra werk in categorie""")
            #                     ],
            #                     className="pretty_container 3 columns",
            #                 ),
            #             ],
            #             id="info-container3",
            #             className="container-display",
            #         ),
            #     ],
            # ),
            # html.Div(
            #     [
            #         html.Div(
            #             [
            #                 dcc.Graph(id="pie_graph"),
            #                 html.Div(
            #                     [
            #                         dbc.Button(
            #                             'Uitleg categorieën',
            #                             id='uitleg_button'
            #                         ),
            #                         html.Div(
            #                             [
            #                                 dcc.Markdown(
            #                                     config.uitleg_categorie
            #                                 )
            #                             ],
            #                             id='uitleg_collapse',
            #                             hidden=True,
            #                         )
            #                     ],
            #                 ),
            #             ],
            #             className="pretty_container column",
            #         ),
            #         html.Div(
            #             dcc.Graph(id="OHW_bakje_graph"),
            #             className="pretty_container column",
            #         ),
            #     ],
            #     className="container-display",
            # ),
            # html.Div(
            #     id='status_table_ext',
            #     className="pretty_container",
            #     # hidden=True,
            # ),
        ],
        id="mainContainer",
        style={"display": "flex", "flex-direction": "column"},
    )
    return page


# Info gauge meters
@app.callback(
    [
     Output("my-gauge-1", "label"),
     Output("my-gauge-1", "max"),
     Output("my-gauge-1", "value"),
     Output("my-gauge-2", "label"),
     Output("my-gauge-2", "max"),
     Output("my-gauge-2", "value"),
     Output("my-gauge-3", "label"),
     Output("my-gauge-3", "max"),
     Output("my-gauge-3", "value"),
     Output("my-gauge-4", "label"),
     Output("my-gauge-4", "max"),
     Output("my-gauge-4", "value"),
    ],
    [
     Input("checklist_filters", "value"),
    ],
)
def update_gauges(value):
    df_l = data_from_DB()
    output = []
    for pname in ['Brielle', 'Dongen', 'Helvoirt', 'Nijmegen']:
        output += [pname]
        output += [len(df_l[pname])]
        output += [df_l[pname]['HasApp_Status'].value_counts()['VOLTOOID']]

    return output


# Globale grafieken
@app.callback(
    [Output("OHW_globaal_graph", "figure"),
     ],
    [Input("checklist_filters", 'value')
     ]
)
def make_barplot(filter_selectie):
    if filter_selectie is None:
        raise PreventUpdate
    df_l = data_from_DB()
    df = df_l[filter_selectie]
    # category = 'global'
    if df.empty:
        raise PreventUpdate
    bar, stats, geo_plot = generate_graph(df)
    return [bar]


@app.callback(
    [Output("geo_plot", "figure"),
     ],
    [Input("checklist_filters", 'value'),
     Input("OHW_globaal_graph", 'clickData'),
     ]
)
def make_geoplot(filter_selectie, clickData):
    if filter_selectie is None:
        raise PreventUpdate
    df_l = data_from_DB()
    df = df_l[filter_selectie]
    if clickData is not None:
        df = df[(df['Opleverstatus'] == int(clickData['points'][0]['x'][1:])) &
                (df['schouwAkkoord'] == int(clickData['points'][0]['curveNumber']))]
    if df.empty:
        raise PreventUpdate
    bar, stats, geo_plot = generate_graph(df)
    return [geo_plot]


# DOWNLOAD FUNCTIES
@app.callback(
    [Output('download-link', 'href'),
     #  Output('download-link1', 'href'),
     #  Output('download-link2', 'href'),
     ],
    [Input("checklist_filters", 'value'),
     #  Input('pie_graph', 'clickData'),
     ]
)
def update_link(filter_selectie):
    if filter_selectie is None:
        raise PreventUpdate
    # if category is None:
    #     cat = config.beschrijving_cat[0]
    # else:
    #     cat = category.get('points')[0].get('label')

    return ['/download_excel?filters={}'.format(filter_selectie),
            # '/download_excel1?filters={}'.format(filter_selectie),
            # '/download_excel2?filters={}'.format(filter_selectie)
            ]

# download categorie
@app.server.route('/download_excel')
def download_excel():
    # category = flask.request.args.get('categorie')
    filter_selectie = flask.request.args.get('filters')
    df = data_from_DB(filter_selectie)
    # version_r = max(df['Datum_WF'].dropna().sum()).replace('-', '_')
    df_table = df

    # Convert df to excel
    strIO = io.BytesIO()
    excel_writer = pd.ExcelWriter(strIO, engine="xlsxwriter")
    df_table.to_excel(excel_writer, sheet_name="sheet1", index=False)
    excel_writer.save()
    strIO.getvalue()
    strIO.seek(0)

    # Name download file
    date = dt.datetime.now().strftime('%d-%m-%Y')
    filename = "Info_project_filters_{}_{}['x']lsx".format(filter_selectie, date)
    return send_file(strIO,
                     attachment_filename=filename,
                     as_attachment=True)


# HELPER FUNCTIES
@cache.memoize()
def data_from_DB():
    df_l = {}
    for p in ['Brielle', 'Dongen', 'Helvoirt', 'Nijmegen']:
        df_l[p] = pd.read_csv(config.files[p], sep=';', encoding='latin-1')

    return df_l


def generate_graph(df):

    bar, stats, df_g = processed_data(df)

    if bar is not None:
        labels = ['s35', 's5', 's1', 's31', 's14']
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

        bar1 = go.Bar(x=labels, y=bar['1'], name='SchouwAkkoord', marker=go.bar.Marker(color='rgb(0, 255, 0)'))
        bar2 = go.Bar(x=labels, y=bar['0'], name='geen SchouwAkkoord', marker=go.bar.Marker(color='rgb(255, 0, 0)'))
        # bar3 = go.Bar(x=labels, y=bar['R3'], name=info_l['R3'])
        # bar4 = go.Bar(x=labels, y=bar['R5'], name=info_l['R5'])
        # bar5 = go.Bar(x=labels, y=bar['R6'], name=info_l['R6'])
        # bar6 = go.Bar(x=labels, y=bar['R7'], name=info_l['R7'])
        # bar7 = go.Bar(x=labels, y=bar['R8'], name=info_l['R8'])
        # bar8 = go.Bar(x=labels, y=bar['R10'], name=info_l['R10'])
        bar = go.Figure(data=[bar1, bar2], layout=go.Layout(barmode='stack', clickmode='event+select'))

    if df_g is not None:
        mapbox_access_token = "pk.eyJ1IjoiamFja2x1byIsImEiOiJjajNlcnh3MzEwMHZtMzNueGw3NWw5ZXF5In0.fk8k06T96Ml9CLGgKmk81w"
        normalized_size = 1  # (df_g['Sleutel']-df_g['Sleutel'].min())/(df_g['Sleutel'].max()-df_g['Sleutel'].min())
        df_g['clr'] = df_g['clr'].astype(int)

        map_data = [
            go.Scattermapbox(
                lat=df_g['Lat'],
                lon=df_g['Long'],
                mode='markers',
                marker=dict(
                    cmax=50,
                    cmin=0,
                    color=df_g['clr'],
                    colorbar=dict(
                        title='Colorbar'
                    ),
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
            title="Overzicht status woningen (groen = afgerond)",
            mapbox=dict(
                accesstoken=mapbox_access_token,
                style="light",
                center=dict(lon=df_g['Long'].mean(), lat=df_g['Lat'].mean()),
                zoom=13,
            ),
        )

        geo_plot = {'data': map_data, 'layout': map_layout}

    return bar, stats, geo_plot


def processed_data(df):
    # bar chart
    df_detail = df[df['HasApp_Status'] != 'VOLTOOID']

    bar = {}
    status_0 = []
    status_1 = []
    for key in df_detail['Opleverstatus'].unique():
        if key != 2:
            status_0 += [len(df_detail[(df_detail['Opleverstatus'] == key) & (df_detail['schouwAkkoord'] == 0)])]
            status_1 += [len(df_detail[(df_detail['Opleverstatus'] == key) & (df_detail['schouwAkkoord'] == 1)])]
    bar['0'] = status_0
    bar['1'] = status_1

    # stats
    stats = {'0': str(round(len(df))),
             '1': str(round(0)),
             '2': str(round(0))}

    # geoplot
    df_g = df.groupby('Sleutel').agg(
        {
            'Sleutel': 'count',
            'Opleverstatus': lambda x: sum(x == 2),
            'X locatie Rol': 'first',
            'Y locatie Rol': 'first'
        }
    )
    # df_g.reset_index(inplace=True)
    df_g['clr'] = df_g['Opleverstatus'] / df_g['Sleutel'] * 100
    df_g['X locatie Rol'] = df_g['X locatie Rol'].str.replace(',', '.').astype(float)
    df_g['Y locatie Rol'] = df_g['Y locatie Rol'].str.replace(',', '.').astype(float)
    df_g['Lat'], df_g['Long'] = from_rd(df_g['X locatie Rol'], df_g['Y locatie Rol'])

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
