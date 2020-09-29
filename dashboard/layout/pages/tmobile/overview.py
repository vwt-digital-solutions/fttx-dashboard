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
             value=jaaroverzicht.get('target', 'n.v.t.')),
        dict(id_="info_globaal_container1", title='Realisatie (FC)', text="HPend gerealiseerd: ",
             value=jaaroverzicht.get('real', 'n.v.t.')),
        dict(id_="info_globaal_container2", title='Planning (VWT)', text="HPend gepland vanaf nu: ",
             value=jaaroverzicht.get('plan', 'n.v.t.')),
        dict(id_="info_globaal_container3", title='Voorspelling (VQD)',
             text="HPend voorspeld vanaf nu: ", value=jaaroverzicht.get('prog', 'n.v.t'),
             className=jaaroverzicht.get("prog_c", 'n.v.t.') + "  column"),
        dict(id_="info_globaal_container5", title='Werkvoorraad HAS',
             value=jaaroverzicht.get('HAS_werkvoorraad', 'n.v.t.')),
        dict(id_="info_globaal_container4", title='Actuele HC / HPend',
             value=jaaroverzicht.get('HC_HPend', 'n.v.t.')),
        dict(id_="info_globaal_container4", title='Ratio <8 weken',
             value=jaaroverzicht.get('ratio_op_tijd', 'n.v.t.')),
     ]
    return [
        global_info_list(jaaroverzicht_list,
                         id="info-container1",
                         className="container-display"),
        html.Div(
          className="container-display",
          children=[figure(graph_id="month-overview", figure=new_component.get_html_overview(has_planning_by('month', client))),
                    figure(graph_id="week-overview", figure=new_component.get_html_overview(has_planning_by('week', client))),
                    figure(container_id="pie_chart_overview_" + client + "_container",
                           graph_id="pie_chart_overview_" + client,
                           figure=pie_chart(client))]
        ),
    ]
