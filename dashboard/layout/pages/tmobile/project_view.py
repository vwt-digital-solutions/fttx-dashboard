import dash_html_components as html

import config
from data.data import completed_status_counts, redenna_by_completed_status
from layout.components.graphs import completed_status_counts_bar
from layout.components.redenna_status_pie import get_fig as redenna_status_pie
from layout.components.figure import figure
import dash_core_components as dcc
colors = config.colors_vwt


def get_html(project_name, client):
    if project_name:
        status_counts = completed_status_counts(project_name, client=client)
        laagbouw_fig = completed_status_counts_bar.get_fig(status_counts.laagbouw,
                                                           title="Status oplevering per fase (LB)")
        hoogbouw_fig = completed_status_counts_bar.get_fig(status_counts.hoogbouw,
                                                           title="Status oplevering per fase (HB & Duplex)")

        redenna_counts = redenna_by_completed_status(project_name, client=client)
        redenna_pie = redenna_status_pie(redenna_counts,
                                         title="Opgegeven reden na",
                                         colors=[
                                             colors['vwt_blue'],
                                             colors['yellow'],
                                             colors['red'],
                                             colors['green']
                                         ]
                                         )

    else:
        laagbouw_fig = {'data': None, 'layout': None}
        hoogbouw_fig = {'data': None, 'layout': None}
        redenna_pie = {'data': None, 'layout': None}

    return [
        dcc.Store(id=f"status-count-filter-{client}"),
        html.Div(
            className="container-display",
            id=f"indicators-{client}"
        ),
        html.Div(
            className="container-display",
            children=[
                figure(figure=laagbouw_fig,
                       container_id=f"status-counts-laagbouw-{client}-container",
                       graph_id=f"status-counts-laagbouw-{client}"),
                figure(figure=hoogbouw_fig,
                       container_id=f"status-counts-hoogbouw-{client}-container",
                       graph_id=f"status-counts-hoogbouw-{client}"),
                figure(container_id="redenna_project_t-mobile_container",
                       graph_id="redenna_project_t-mobile",
                       figure=redenna_pie)
            ]
        )
    ]
