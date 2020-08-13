import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html

from layout.components.header import header
from config_pages import config_pages

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
            header("Hoofdpagina FttX"),

            dbc.Jumbotron(
                [
                    html.H1("FttX", className="display-3"),
                    html.Hr(className="my-2"),
                    html.P("""
                        Op deze pagina komt een totaal overzicht voor de verschillende projecten binnen FttX.
                        Gebruik de knoppen hier onder om naar speciefieke projecten te gaan.
                        """),
                    html.P(dbc.ButtonGroup([
                        dbc.Button(page_config['name'], href=page_config['link'][0], color="primary")
                        for page_id, page_config in config_pages.items()
                        if "/" not in page_config['link']
                    ], size="lg"), className="lead"),
                ]
            )
        ],
        id="mainContainer",
        style={"display": "flex", "flex-direction": "column"},
    )
    return page
