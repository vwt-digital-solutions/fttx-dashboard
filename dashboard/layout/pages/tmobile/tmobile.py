import dash_core_components as dcc
import dash_html_components as html

from data.data import has_planning_by_week, has_planning_by_month
from layout.components.figure import figure
from layout.components.global_info_list import global_info_list
from layout.components.header import header
from layout.pages.tmobile import planning_has_graph, new_component
from data import collection
from data.graph import pie_chart

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
                 collection="Data", client="t-mobile", graph_name="voorraadvormend")['all'])),
    ]

    page = html.Div(
        [
            dcc.Store(id="aggregate_data",
                      data=None),
            dcc.Store(id="aggregate_data2",
                      data=None),
            dcc.Store(id="aggregate_data3",
                      data=None),
            header("Status projecten T-Mobile in 2020"),
            global_info_list(jaaroverzicht_list,
                             id="info-container1",
                             className="container-display"),
            html.Div(
                className="container-display",
                children=[
                    new_component.get_html(value=379,
                                           previous_value=402,
                                           title="Te laat",
                                           sub_title="> 12 weken",
                                           font_color="red"),
                    new_component.get_html(value=42,
                                           previous_value=38,
                                           title="Nog beperkte tijd",
                                           sub_title="> 8 weken < 12 weken",
                                           font_color="orange"),
                    new_component.get_html(value=823,
                                           previous_value=789,
                                           title="Op tijd",
                                           sub_title="< 8 weken",
                                           font_color="green"),
                ]
            ),
            html.Div(
                className="container-display",
                children=[planning_has_graph.get_html_week(has_planning_by_week()),
                          planning_has_graph.get_html_month(has_planning_by_month())]
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
        ],
        id="mainContainer",
        style={"display": "flex", "flex-direction": "column"},
    )
    return page
