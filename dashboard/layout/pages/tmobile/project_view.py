import dash_html_components as html

from data.data import completed_status_counts
from layout.components.graphs import completed_status_counts_bar
from data.graph import pie_chart
from layout.components.figure import figure


def get_html(project_name):
    status_counts = completed_status_counts(project_name)

    if project_name:
        pie_chart_project = pie_chart(client='t-mobile', key=project_name)
    else:
        pie_chart_project = {'data': None, 'layout': None}

    return [
        html.Div(
            className="container-display",
            children=[
                completed_status_counts_bar.get_html(status_counts.laagbouw,
                                                     title="Status oplevering per fase (LB)"),
                completed_status_counts_bar.get_html(status_counts.hoogbouw,
                                                     title="Status oplevering per fase (HB & Duplex)"),
                figure(container_id="pie_chart_project_t-mobile_container",
                       graph_id="pie_chart_project_t-mobile_container",
                       className="pretty_container column",
                       figure=pie_chart_project)
            ]
        )
    ]
