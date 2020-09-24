from data.data import has_planning_by
from layout.components.figure import figure
from layout.components.global_info_list import global_info_list
from layout.pages.tmobile import new_component
from data import collection
from data.graph import pie_chart

import dash_html_components as html


def get_html(client):
    jaaroverzicht = collection.get_document(collection="Data", graph_name="jaaroverzicht", client=client)
    jaaroverzicht_list = [
        dict(id_="info_globaal_container0",
             title='Outlook',
             text="HPend afgesproken: ",
             value='10000'),
        dict(id_="info_globaal_container1", title='Realisatie (FC)', text="HPend gerealiseerd: ",
             value=jaaroverzicht['real']),
        dict(id_="info_globaal_container2", title='Planning (VWT)', text="HPend gepland vanaf nu: ",
             value=jaaroverzicht['plan']),
        dict(id_="info_globaal_container3", title='Voorspelling (VQD)',
             text="HPend voorspeld vanaf nu: ", value='1000'),
        dict(id_="info_globaal_container5", title='Werkvoorraad HAS',
             value=str(collection.get_document(
                 collection="Data", client=client, graph_name="voorraadvormend")['all'])),
        dict(id_="info_globaal_container4", title='Actuele HC / HPend',
             value='n.v.t.'),
        dict(id_="info_globaal_container4", title='Ratio <8 weken',
             value='0.66'),
    ]

    return [
        global_info_list(jaaroverzicht_list,
                         id="info-container1",
                         className="container-display"),
        html.Div(
            className="container-display",
            children=[figure(graph_id="month-overview", figure=new_component.get_html_overview(has_planning_by('month', client))),
                      figure(graph_id="week-overview", figure=new_component.get_html_overview(has_planning_by('week', client))),
                      figure(container_id="pie_chart_overview_t-mobile_container",
                             graph_id="pie_chart_overview_t-mobile",
                             figure=pie_chart('t-mobile'))]
        ),
    ]
