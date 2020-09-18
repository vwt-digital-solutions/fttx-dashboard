from data.data import has_planning_by
from layout.components.figure import figure
from layout.components.global_info_list import global_info_list
from layout.pages.tmobile import planning_has_graph
from data import collection
from data.graph import pie_chart

import dash_html_components as html


def get_html(client):
    jaaroverzicht_list = [
        dict(id_="info_globaal_container0",
             title='Outlook (KPN)',
             text="HPend afgesproken: ",
             value='31991'),
        dict(id_="info_globaal_container1", title='Realisatie (FC)', text="HPend gerealiseerd: ",
             value='5279'),
        dict(id_="info_globaal_container2", title='Planning (VWT)', text="HPend gepland vanaf nu: ",
             value='1014'),
        dict(id_="info_globaal_container3", title='Voorspelling (VQD)',
             text="HPend voorspeld vanaf nu: ", value='1400'),
        dict(id_="info_globaal_container4", title='Ratio <8 weken',
             value='0.66'),
        dict(id_="info_globaal_container5", title='Werkvoorraad HAS',
             value=str(collection.get_document(
                 collection="Data", client=client, graph_name="voorraadvormend")['all'])),
    ]

    return [
        global_info_list(jaaroverzicht_list,
                         id="info-container1",
                         className="container-display"),
        html.Div(
            className="container-display",
            children=[planning_has_graph.get_html_overview(has_planning_by(period='month', client=client)),
                      planning_has_graph.get_html_overview(has_planning_by(period='week', client=client)),
                      figure(container_id="pie_chart_overview_t-mobile_container",
                             graph_id="pie_chart_overview_t-mobile",
                             figure=pie_chart('t-mobile'))]
        )
    ]
