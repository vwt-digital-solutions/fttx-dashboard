import dash_core_components as dcc
import dash_html_components as html

from layout.components.global_info_list import global_info_list
from layout.components.header import header
from layout.pages.tmobile.sales_graph import get_html


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
             value='1000'),
        dict(id_="info_globaal_container1", title='Realisatie (FC)', text="HPend gerealiseerd: ",
             value='1200'),
        dict(id_="info_globaal_container2", title='Planning (VWT)', text="HPend gepland vanaf nu: ",
             value='1300'),
        dict(id_="info_globaal_container3", title='Voorspelling (VQD)',
             text="HPend voorspeld vanaf nu: ", value='1400'),
        dict(id_="info_globaal_container4", title='Actuele HC / HPend',
             value='1500'),
        dict(id_="info_globaal_container5", title='Werkvoorraad HAS',
             value='1600'),
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
                children=[get_html('Sales, HAsses & Activations (by week)'),
                          get_html('Sales, HAsses & Activations (by month)', flag=2)]
            )
        ],
        id="mainContainer",
        style={"display": "flex", "flex-direction": "column"},
    )
    return page
