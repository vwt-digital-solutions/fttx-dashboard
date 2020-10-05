from data.data import no_graph
from layout.components.figure import figure
from data import collection
from data.graph import ftu_table

import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import config

colors = config.colors_vwt


def get_html(client):
    # jaaroverzicht = collection.get_document(collection="Data", graph_name="jaaroverzicht", client=client)

    # jaaroverzicht_list = [
    #     dict(id_="info_globaal_container0",
    #          title='Outlook',
    #          text="HPend afgesproken: ",
    #          value=jaaroverzicht.get('target', 'n.v.t.')),
    #     dict(id_="info_globaal_container1", title='Realisatie (FC)', text="HPend gerealiseerd: ",
    #          value=jaaroverzicht.get('real', 'n.v.t.')),
    #     dict(id_="info_globaal_container2", title='Planning (VWT)', text="HPend gepland vanaf nu: ",
    #          value=jaaroverzicht.get('plan', 'n.v.t.')),
    #     dict(id_="info_globaal_container3", title='Voorspelling (VQD)',
    #          text="HPend voorspeld vanaf nu: ", value=jaaroverzicht.get('prog', 'n.v.t'),
    #          className=jaaroverzicht.get("prog_c", 'n.v.t.') + "  column"),
    #     dict(id_="info_globaal_container5", title='Werkvoorraad HAS',
    #          value=jaaroverzicht.get('HAS_werkvoorraad', 'n.v.t.')),
    #     dict(id_="info_globaal_container4", title='Actuele HC / HPend',
    #          value=jaaroverzicht.get('HC_HPend', 'n.v.t.')),
    #     dict(id_="info_globaal_container4", title='Ratio <8 weken',
    #          value=jaaroverzicht.get('ratio_op_tijd', 'n.v.t.')),
    #  ]
    return [
        html.Div(
            children=[],
            id=f"info-container-{client}",
        ),
        html.Div(
            className="container-display",
            children=[figure(graph_id=f"month-overview-{client}", figure=no_graph(title="Jaaroverzicht", text='Loading...')),
                      figure(graph_id=f"week-overview-{client}", figure=no_graph(title="Maandoverzicht", text='Loading...')),
                      figure(container_id="pie_chart_overview_t-mobile_container",
                             graph_id="pie_chart_overview_t-mobile",
                             figure=no_graph(title="Opgegeven reden na", text='Loading...'))]
        ),
    ]


def get_search_bar(client):
    return [
                html.Div(
                    [dcc.Dropdown(id='project-dropdown-' + client,
                                  options=collection.get_document(collection="Data", client=client,
                                                                  graph_name="project_names")['filters'],
                                  value=None)],
                    className="two-third column",
                ),
                html.Div(
                    [
                        dbc.Button('Reset overzicht', id='overview-reset',
                                   n_clicks=0,
                                   style={"margin-left": "10px",
                                          "margin-right": "55px",
                                          'background-color': colors['vwt_blue']})
                    ]
                ),
                html.Div(
                    [
                        dbc.Button('Terug naar overzicht alle projecten',
                                   id='overzicht-button-' + client,
                                   style={'background-color': colors['vwt_blue']})
                    ],
                    className="one-third column",
                ),
            ]


def get_performance(client):
    ftu_data = collection.get_document(collection="Data", graph_name="project_dates", client=client)
    table = ftu_table(ftu_data)
    return [
                figure(container_id="graph_speed_c",
                       graph_id="project_performance",
                       figure=collection.get_graph(client=client,
                                                   graph_name="project_performance")),
                html.Div([
                    html.Div(
                        table,
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
            ]
