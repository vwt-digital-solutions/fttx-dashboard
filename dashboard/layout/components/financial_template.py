import dash_core_components as dcc
import dash_html_components as html

from data.data import no_graph
from layout.components.figure import figure


def financial_template(client):
    return [
        dcc.Store(id=f"financial-data-{client}",
                  data=None),
        dcc.Store(id=f"progress-over-time-data-{client}",
                  data=None),
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
                       ),
                figure(figure=no_graph(text="Geen selectie"),
                       container_id=f"progress-over-time-{client}-container",
                       graph_id=f"progress-over-time-{client}",
                       title="Verloop"
                       )
            ]
        )
    ]
