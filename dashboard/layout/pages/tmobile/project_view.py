import dash_html_components as html

from data.data import no_graph
from layout.components.figure import figure
from layout.pages.tmobile import new_component
import dash_core_components as dcc


def get_html(client):
    redenna_pie = no_graph(title="Opgegeven reden na", text='Loading...')
    laagbouw_fig = no_graph(title="Status oplevering per fase (LB)", text='Loading...')
    hoogbouw_fig = no_graph(title="Status oplevering per fase (HB)", text='Loading...')

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
                figure(container_id="redenna_project_tmobile_container",
                       graph_id="redenna_project_tmobile",
                       figure=redenna_pie)
            ]
        )
    ]
