import dash_html_components as html
import dash_core_components as dcc


def figure(container_id="", graph_id="", className="pretty_container column", figure=None):
    return html.Div(
        [
            dcc.Graph(id=graph_id, figure=figure),
        ],
        className=className,
        hidden=False,
        id=container_id,
        n_clicks=0
    )
