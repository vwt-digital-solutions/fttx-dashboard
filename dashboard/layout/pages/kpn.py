import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html

from data import collection
from layout.components.figure import figure
from data.graph import pie_chart, ftu_table
from layout.components.global_info_list import global_info_list
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
    jaaroverzicht = collection.get_document(collection="Data", graph_name="jaaroverzicht", client="KPN")

    jaaroverzicht_list = [
        dict(id_="info_globaal_container0",
             title='Outlook (KPN)',
             text="HPend afgesproken: ",
             value=jaaroverzicht['target']),
        dict(id_="info_globaal_container1", title='Realisatie (FC)', text="HPend gerealiseerd: ",
             value=jaaroverzicht['real']),
        dict(id_="info_globaal_container2", title='Planning (VWT)', text="HPend gepland vanaf nu: ",
             value=jaaroverzicht['plan']),
        dict(id_="info_globaal_container3", title='Voorspelling (VQD)',
             text="HPend voorspeld vanaf nu: ", value=jaaroverzicht['prog'],
             className=jaaroverzicht["prog_c"] + "  column"),
        dict(id_="info_globaal_container4", title='Actuele HC / HPend',
             value=jaaroverzicht['HC_HPend']),
        dict(id_="info_globaal_container5", title='Werkvoorraad HAS',
             value=jaaroverzicht['HAS_werkvoorraad']),
    ]

    page = html.Div(
        [
            dcc.Store(id="aggregate_data",
                      data=None),
            dcc.Store(id="aggregate_data2",
                      data=None),
            dcc.Store(id="aggregate_data3",
                      data=None),
            header("Status projecten KPN in 2020"),
            global_info_list(jaaroverzicht_list,
                             id="info-container1",
                             className="container-display"),
            html.Div(
                [
                    figure(container_id="graph_targets_M_container",
                           graph_id="graph_targets_M",
                           figure=collection.get_graph(client="KPN", graph_name="graph_targets_M")),
                    figure(container_id="graph_targets_W_container",
                           graph_id="graph_targets_W",
                           figure=collection.get_graph(client="KPN", graph_name="graph_targets_W")),
                    html.Div(
                        [dcc.Graph(id="Pie_NA_o", figure=pie_chart())],
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
                           figure=collection.get_graph(client="KPN",
                                                       graph_name="project_performance")),
                    html.Div([
                        html.Div(id='ww_c',
                                 children=dcc.Input(id='ww', value=' ', type='text'),
                                 className="pretty_container column",
                                 hidden=False,
                                 ),
                        html.Div(
                            ftu_table(),
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
                                      options=collection.get_document(collection="Data", client="KPN",
                                                                      graph_name="project_names")['filters'],
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
