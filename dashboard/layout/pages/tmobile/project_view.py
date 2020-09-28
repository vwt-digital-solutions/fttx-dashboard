import dash_html_components as html

from data.data import completed_status_counts
from layout.components.graphs import completed_status_counts_bar
from data.graph import pie_chart
from layout.components.figure import figure
from layout.pages.tmobile import new_component
import dash_core_components as dcc


def get_html(project_name, client):
    if project_name:
        pie_chart_project = pie_chart(client=client, key=project_name)
        status_counts = completed_status_counts(project_name)
        laagbouw_fig = completed_status_counts_bar.get_fig(status_counts.laagbouw,
                                                           title="Status oplevering per fase (LB)")
        hoogbouw_fig = completed_status_counts_bar.get_fig(status_counts.hoogbouw,
                                                           title="Status oplevering per fase (HB & Duplex)")
    else:
        pie_chart_project = {'data': None, 'layout': None}
        laagbouw_fig = {'data': None, 'layout': None}
        hoogbouw_fig = {'data': None, 'layout': None}

    return [
        dcc.Store(id=f"status-count-filter-{client}"),
        html.Div(
            className="container-display",
            children=[
                new_component.get_html(value=100,
                                       previous_value=110,
                                       title="Order te laat",
                                       sub_title="> 12 weken",
                                       font_color="red"),
                new_component.get_html(value=100,
                                       previous_value=90,
                                       title="Order nog beperkte tijd",
                                       sub_title="> 8 weken < 12 weken",
                                       font_color="orange"),
                new_component.get_html(value=100,
                                       previous_value=110,
                                       title="Order op tijd",
                                       sub_title="< 8 weken",
                                       font_color="green"),
            ]
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
                figure(container_id="pie_chart_project_t-mobile_container",
                       graph_id="pie_chart_project_t-mobile_container",
                       figure=pie_chart_project)
            ]
        )
    ]
