import dash_bootstrap_components as dbc
import dash_html_components as html

from data import api
from layout.components.figure import figure
from layout.components.graphs.no_graph import no_graph


def financial_template(client):

    datum_baan = api.get("/Graphs?id=update_date_baan_realisation")[0]["date"][
        0:-4
    ].replace("T", " ")
    explain_barchart = f"""
    Deze barchart geeft de financiele status weer op {datum_baan}. We zien hier wat er is begroot, hoeveel hiervan
    gerealiseerd is, en ook hoeveel we operationeel al gedaan hebben.

    De operationele voortgang is berekend door de voortgang van het aantal aangesloten huizen te vermenigvuldigen
    met de gemiddelde kosten voor het aansluiten van een huis.
    """

    return [
        html.Div(
            className="container-display ml-3",
            id=f"finance-warnings-{client}",
            children=[],
        ),
        html.Div(
            className="container-display",
            children=[
                figure(
                    figure=no_graph(),
                    container_id=f"budget-bar-category-{client}-container",
                    graph_id=f"budget-bar-category-{client}",
                    title="Begroting/Prognose einde werk/Realisatie",
                ),
                dbc.Tooltip(
                    explain_barchart,
                    id=f"{client}-hover-finance-main-barchart",
                    target=f"budget-bar-category-{client}-container-title",
                    placement="below",
                    style={"font-size": 12},
                ),
            ],
        ),
        html.Div(
            className="container-display",
            children=[
                figure(
                    figure=no_graph(text="Geen selectie"),
                    container_id=f"budget-bar-sub-category-{client}-container",
                    graph_id=f"budget-bar-sub-category-{client}",
                    title="Begroting/Prognose einde werk/Realisatie",
                ),
                figure(
                    figure=no_graph(text="Geen selectie"),
                    container_id=f"progress-over-time-{client}-container",
                    graph_id=f"progress-over-time-{client}",
                    title="Verloop",
                ),
            ],
        ),
    ]
