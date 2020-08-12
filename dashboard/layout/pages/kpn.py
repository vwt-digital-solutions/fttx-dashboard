import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html

from layout.components.figure import figure
from layout.components.global_info import global_info
from data.graph import graph
from data.figure import figure_data
from data.jaaroverzicht import jaaroverzicht_data
from layout.components.header import header

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
            header("Status projecten KPN in 2020"),
            html.Div(
                [
                    global_info("info_globaal_container0",
                                title='Outlook (KPN)',
                                text="HPend afgesproken: ",
                                value=jaaroverzicht_data('target')),
                    global_info("info_globaal_container1", title='Realisatie (FC)', text="HPend gerealiseerd: ",
                                value=jaaroverzicht_data('real')),
                    global_info("info_globaal_container2", title='Planning (VWT)', text="HPend gepland vanaf nu: ",
                                value=jaaroverzicht_data('plan')),
                    global_info("info_globaal_container3", title='Voorspelling (VQD)',
                                text="HPend voorspeld vanaf nu: ", value=jaaroverzicht_data('prog'),
                                className=jaaroverzicht_data("prog_c") + "  column"),
                    global_info("info_globaal_container4", title='Actuele HC / HPend',
                                value=jaaroverzicht_data('HC_HPend')),
                    global_info("info_globaal_container5", title='Werkvoorraad HAS',
                                value=jaaroverzicht_data('HAS_werkvoorraad')),
                ],
                id="info-container1",
                className="container-display",
            ),
            html.Div(
                [
                    figure(container_id="graph_targets_overall_c",
                           graph_id="graph_targets_ov",
                           figure=figure_data('graph_targets_W')),
                    figure(container_id="graph_targets_overallM_c",
                           graph_id="graph_targets_m",
                           figure=figure_data('graph_targets_M')),
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
                    figure(container_id="graph_speed_c",
                           graph_id="project_performance",
                           figure=figure_data('project_performance')),
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
