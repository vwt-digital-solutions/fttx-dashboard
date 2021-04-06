import dash_html_components as html
import dash_bootstrap_components as dbc
from data import api

from data.data import no_graph
from layout.components.figure import figure


def financial_template(client):

    datum_baan = api.get('/Graphs?id=update_date_baan_realisation')[0]['date'][0:-10].replace('T', ' ')
    explain_barchart_1 = f"""
    Deze barchart geeft de financiele status weer op {datum_baan}. We zien hier wat er is begroot, hoeveel hiervan
    is gerealiseerd, en wat er operationeel is gedaan."""
    explain_barchart_2 = """
    De operationele voortgang is berekend door het aantal aangesloten huizen te vermenigvuldigen
    met de gemiddelde kosten voor het aansluiten van een huis in een bepaalde categorie / sub-categorie.
    """

    return [
        html.Div(
            className="container-display ml-3",
            id=f"finance-warnings-{client}",
            children=[

            ]
        ),
        html.Div(
            className="container-display",
            children=[
                figure(figure=no_graph(),
                       container_id=f"budget-bar-category-{client}-container",
                       graph_id=f"budget-bar-category-{client}",
                       title="Begroting/Prognose einde werk/Realisatie"
                       ),
                dbc.Tooltip(children=[html.P(explain_barchart_1),
                                      html.Br(),
                                      html.P(explain_barchart_2)],
                            id=f"{client}-hover-finance-main-barchart",
                            target=f"budget-bar-category-{client}-container-title",
                            placement="below",
                            style={"font-size": 12}),
            ]
        ),
        html.Div(
            className="container-display",
            children=[
                figure(figure=no_graph(text="Geen selectie"),
                       container_id=f"budget-bar-sub-category-{client}-container",
                       graph_id=f"budget-bar-sub-category-{client}",
                       title="Begroting/Prognose einde werk/Realisatie"
                       ),
                figure(figure=no_graph(text="Geen selectie"),
                       container_id=f"progress-over-time-{client}-container",
                       graph_id=f"progress-over-time-{client}",
                       title="Verloop"
                       )
            ]
        )
    ]
