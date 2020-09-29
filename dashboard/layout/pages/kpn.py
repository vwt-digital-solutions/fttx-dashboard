import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html

from data import collection
from layout.components.figure import figure
from data.graph import ftu_table
from layout.components.header import header
from layout.pages.tmobile import overview
import config

colors = config.colors_vwt


# APP LAYOUT
def get_body():
    client = 'kpn'
    page = html.Div(
        [
            dcc.Store(id="aggregate_data",
                      data=None),
            dcc.Store(id="aggregate_data2",
                      data=None),
            header("Status projecten KPN in 2020"),
            html.Div(
                id="tmobile-overview",
                children=overview.get_html(client),
            ),
            html.Div(
                [
                    html.Div(
                        [dcc.Dropdown(id='project-dropdown',
                                      options=collection.get_document(collection="Data", client="kpn",
                                                                      graph_name="project_names")['filters'],
                                      value=None)],
                        className="two-third column",
                    ),
                    html.Div(
                        [dbc.Button('Terug naar overzicht alle projecten',
                                    id='overzicht_button',
                                    style={'background-color': colors['vwt_blue']})],
                        className="one-third column",
                    ),
                ],
                className="container-display",
                id="title",
            ),
            html.Div(
                [

                    figure(container_id="graph_speed_c",
                           graph_id="project_performance",
                           figure=collection.get_graph(client="kpn",
                                                       graph_name="project_performance")),
                    html.Div([
                        html.Div(
                            ftu_table(),
                            id='FTU_table_c',
                            className="pretty_container column",
                            hidden=False,
                        ),
                        html.Div(id='ww_c',
                                 children=dcc.Input(id='ww', value=' ', type='text'),
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
            # html.Div(
            #     [dbc.Button('Project details [eerste 3000 resultaten]', id='detail_button')],
            #     className="one-third column"
            # ),
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
