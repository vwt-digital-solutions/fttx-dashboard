import dash_html_components as html

from data.data import completed_status_counts
from layout.components.graphs import completed_status_counts_bar


def get_html(project_name):
    status_counts = completed_status_counts(project_name)

    return [
        html.Div(
            className="container-display",
            children=[
                completed_status_counts_bar.get_html(status_counts.laagbouw,
                                                     title="Status oplevering per fase (LB)"),
                completed_status_counts_bar.get_html(status_counts.hoogbouw,
                                                     title="Status oplevering per fase (HB & Duplex)"),
            ]
        )
    ]
