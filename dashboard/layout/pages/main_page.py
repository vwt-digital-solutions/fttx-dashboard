import dash_bootstrap_components as dbc
import dash_html_components as html

from layout.components.header import header
from config_pages import config_pages
import config

colors = config.colors_vwt


# APP LAYOUT
def get_body(client="", project=''):
    page = html.Div(
        [
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
                        dbc.Button(page_config['name'], href=page_config['link'][0], style={'background-color': colors['vwt_blue']})
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
