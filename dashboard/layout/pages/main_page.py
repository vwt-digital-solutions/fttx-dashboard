import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html

from app import app
from components.global_info import global_info
from components.graph import graph, graph_new, jaaroverzicht_graph

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
    graph_new("jaaroverzicht")
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
                                           children='Laatste data update: ' + graph(85, None, None),
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
                    global_info("info_globaal_container0", title='Outlook (KPN)', text="HPend afgesproken: ",
                                value=jaaroverzicht_graph('target')),
                    global_info("info_globaal_container1", title='Realisatie (FC)', text="HPend gerealiseerd: ",
                                value=jaaroverzicht_graph('real')),
                    global_info("info_globaal_container2", title='Planning (VWT)', text="HPend gepland vanaf nu: ",
                                value=jaaroverzicht_graph('plan')),
                    global_info("info_globaal_container3", title='Voorspelling (VQD)',
                                text="HPend voorspeld vanaf nu: ", value=jaaroverzicht_graph('prog'),
                                className=jaaroverzicht_graph("prog_c") + "  column"),
                    global_info("info_globaal_container4", title='Actuele HC / HPend',
                                value=jaaroverzicht_graph('HC_HPend')),
                    global_info("info_globaal_container5", title='Werkvoorraad HAS',
                                value=jaaroverzicht_graph('HAS_werkvoorraad')),
                ],
                id="info-container1",
                className="container-display",
            ),
            html.Div(
                [
                    html.Div(
                        [dcc.Graph(id='graph_targets_ov', figure=graph(42, None, None))],
                        id='graph_targets_overall_c',
                        className="pretty_container column",
                        hidden=False,
                    ),
                    html.Div(
                        [dcc.Graph(id='graph_targets_m', figure=graph(41, None, None))],
                        id='graph_targets_overallM_c',
                        className="pretty_container column",
                        hidden=False,
                    ),
                    html.Div(
                        [dcc.Graph(id="Pie_NA_o", figure=graph(11, None, None))],
                        id='Pie_NA_oid',
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
                        [dcc.Graph(figure=graph(2, None, None),
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
                            graph(9, None, None),
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
                                      options=graph(3, None, None),
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
                    html.Div(
                        [dcc.Graph(id="Pie_NA_c")],
                        id='Pie_NA_cid',
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
