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
            html.Div(
                className="container-display",
                children=[
                    figure(figure=no_graph(),
                           container_id=f"budget-bar-category-{client}-container",
                           graph_id=f"budget-bar-category-{client}",
                           title="Begroting/Prognose einde werk/Realisatie"
                           )
                ]
            ),
            html.Div(
                className="container-display",
                children=[
                    figure(figure=no_graph(text="Geen selectie"),
                           container_id=f"budget-bar-sub-category-{client}-container",
                           graph_id=f"budget-bar-sub-category-{client}",
                           title="Begroting/Prognose einde werk/Realisatie"
                           )
                ]
            )
        ]
