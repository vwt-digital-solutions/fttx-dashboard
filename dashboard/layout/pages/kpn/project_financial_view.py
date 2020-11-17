import dash_html_components as html
import dash_core_components as dcc

from app import toggles
from data.data import no_graph
from layout.components.figure import figure

if toggles.financial_view:
    def get_html(client):
        return [
            dcc.Store(id=f"financial-data-{client}",
                      data=None),
            html.H1(f"Financien voor {client}"),
            html.Div(
                className="container-display",
                children=[
                    figure(figure=no_graph("Graph1"),
                           container_id=f"budget-bar-{client}-container",
                           graph_id=f"budget-bar-{client}",
                           title="Begroting/Prognose einde werk/Realisatie"
                           )
                ]
            )
        ]
