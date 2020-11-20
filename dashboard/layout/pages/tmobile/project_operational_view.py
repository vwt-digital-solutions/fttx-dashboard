import dash_html_components as html

from data.data import no_graph
from layout.components.figure import figure
import dash_core_components as dcc
import config
colors = config.colors_vwt


def get_html(client):
    return [
        dcc.Store(id=f"status-count-filter-{client}"),
        dcc.Store(id=f"indicator-data-{client}"),
        html.Div(
            className="container-display",
            id=f"indicators-{client}"
        ),
        html.Div(
            className="container-display",
            children=[
                figure(figure=no_graph(title="Status oplevering per fase (LB)", text='Loading...'),
                       container_id=f"status-counts-laagbouw-{client}-container",
                       graph_id=f"status-counts-laagbouw-{client}"),
                figure(figure=no_graph(title="Status oplevering per fase (HB)", text='Loading...'),
                       container_id=f"status-counts-hoogbouw-{client}-container",
                       graph_id=f"status-counts-hoogbouw-{client}"),
                figure(container_id=f"redenna_project_{client}_container",
                       graph_id=f"redenna_project_{client}",
                       figure=no_graph(title="Opgegeven reden na", text='Loading...'))
            ]
        )
    ]