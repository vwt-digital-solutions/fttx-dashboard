from data.data import has_planning_by_week, has_planning_by_month
from layout.components.figure import figure
from layout.components.global_info_list import global_info_list
from layout.pages.tmobile import planning_has_graph, new_component, redenna_pie
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
            children=[
                new_component.get_html(value=379,
                                       previous_value=402,
                                       title="Order te laat",
                                       sub_title="> 12 weken",
                                       font_color="red"),
                new_component.get_html(value=42,
                                       previous_value=38,
                                       title="Order nog beperkte tijd",
                                       sub_title="> 8 weken < 12 weken",
                                       font_color="orange"),
                new_component.get_html(value=823,
                                       previous_value=789,
                                       title="Order op tijd",
                                       sub_title="< 8 weken",
                                       font_color="green"),
            ]
        ),
        html.Div(
            className="container-display",
            children=[planning_has_graph.get_html_week(has_planning_by_week(client=client)),
                      planning_has_graph.get_html_month(has_planning_by_month(client=client)),
                      redenna_pie.get_html(collection.get_document(collection="Data",
                                                                   client=client,
                                                                   graph_name="redenna_by_week"),
                                           "2020-08-31", graph_id="redenna_by_week")
                      ]
        ),
        html.Div(
            className="container-display",
            children=[
                figure(container_id="pie_chart_overview_t-mobile_container",
                       graph_id="pie_chart_overview_t-mobile",
                       figure=pie_chart('t-mobile')
                       )
            ],
        )
    ]
