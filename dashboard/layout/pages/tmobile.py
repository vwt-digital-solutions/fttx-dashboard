import dash_core_components as dcc
import dash_html_components as html

from layout.components.global_info import global_info
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
            header("Status projecten T-Mobile in 2020"),
            html.Div(
                [
                    global_info("tmobile-voorraadvormend",
                                title='Werkvoorraad HAS',
                                value="1264"),

                ],
                id="info-container1",
                className="container-display",
            )
        ],
        id="mainContainer",
        style={"display": "flex", "flex-direction": "column"},
    )
    return page
