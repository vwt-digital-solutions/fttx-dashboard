import dash_html_components as html
import dash_core_components as dcc


def figure(container_id="", graph_id="", className="pretty_container column", figure=None, title="", subtitle=""):
    return html.Div(
        [
            html.H5(title, id=f"{container_id}-title"),
            html.H6(subtitle, id=f"{container_id}-subtitle"),
            dcc.Graph(id=graph_id, figure=figure),
        ],
        className=className,
        hidden=False,
        id=container_id,
        n_clicks=0
    )
