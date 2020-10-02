import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html

from data import collection
from layout.components.figure import figure
from data.graph import pie_chart, ftu_table
from layout.components.global_info_list import global_info_list
from layout.components.header import header
from layout.pages.tmobile import new_component
from data.data import has_planning_by
from data.graph import info_table as graph_info_table
import config

colors = config.colors_vwt


# APP LAYOUT
def get_body():
    jaaroverzicht = collection.get_document(collection="Data", graph_name="jaaroverzicht", client="kpn")

    jaaroverzicht_list = [
        dict(id_="info_globaal_container0",
             title='Outlook',
             text="HPend afgesproken: ",
             value=jaaroverzicht['target']),
        dict(id_="info_globaal_container1", title='Realisatie (FC)', text="HPend gerealiseerd: ",
             value=jaaroverzicht['real']),
        dict(id_="info_globaal_container2", title='Planning (VWT)', text="HPend gepland vanaf nu: ",
             value=jaaroverzicht['plan']),
        dict(id_="info_globaal_container3", title='Voorspelling (VQD)',
             text="HPend voorspeld vanaf nu: ", value=jaaroverzicht['prog'],
             className=jaaroverzicht["prog_c"] + "  column"),
        dict(id_="info_globaal_container4", title='Werkvoorraad HAS',
             value=jaaroverzicht['HAS_werkvoorraad']),
        dict(id_="info_globaal_container5", title='Actuele HC / HPend',
             value=jaaroverzicht['HC_HPend']),
        dict(id_="info_globaal_container6", title='Ratio <8 weken',
             value='n.v.t.'),

    ]

    page = html.Div(
        [
            dcc.Store(id="aggregate_data",
                      data=None),
            dcc.Store(id="aggregate_data2",
                      data=None),
            header("Status projecten KPN in 2020"),
            global_info_list(jaaroverzicht_list,
                             id="info-container1",
                             className="container-display"),
            html.Div(
                [
                    figure(container_id="graph_targets_M_container",
                           graph_id="graph_targets_M",
                           figure=new_component.get_html_overview(has_planning_by('month', 'kpn'))),
                    figure(container_id="graph_targets_W_container",
                           graph_id="graph_targets_W",
                           figure=new_component.get_html_overview(has_planning_by('week', 'kpn'))),
                    figure(container_id="pie_chart_overview_kpn_container",
                           graph_id="pie_chart_overview_kpn",
                           figure=pie_chart('kpn')),
                ],
                id="main_graphs0",
                className="container-display",
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
                id='table_info',
                className="container-display",
                children=[
                    new_component.get_html(value=100,
                                           previous_value=None,
                                           title="Target (outlook)",
                                           # sub_title="> 12 weken",
                                           font_color="green"),
                    new_component.get_html(value=100,
                                           previous_value=90,
                                           title="Realisatie",
                                           # sub_title="> 8 weken < 12 weken",
                                           font_color="green"),
                    new_component.get_html(value=100,
                                           previous_value=110,
                                           title="Delta: Realisatie - Target",
                                           # sub_title="< 8 weken",
                                           font_color="green"),
                    new_component.get_html(value=100,
                                           previous_value=None,
                                           title="HC / HPend",
                                           # sub_title="< 8 weken",
                                           font_color="green"),
                    new_component.get_html(value=100,
                                           previous_value=None,
                                           title="Errors FC - BC",
                                           # sub_title="< 8 weken",
                                           font_color="green"),
                ]
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
                        id='table_info2',
                        className="pretty_container column",
                        hidden=False,
                        children=graph_info_table()
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
