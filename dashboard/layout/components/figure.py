import dash_html_components as html
import dash_core_components as dcc


def figure(container_id: str = "",
           graph_id: str = "",
           className: str = "pretty_container column",
           figure=None,
           title: str = "",
           subtitle: str = ""):
    """
    Creates a figure for plotly dash wrapped in our own structure. The graph is inside a html.Div which also contains
    an optional title and subtitle. Both the container_id and graph_id are optionally supplied in this function.
    These id's allow to use this figure in dash callbacks.

    Args:
        container_id (str): The id for the html element containing the figure and titles.
        graph_id (str): The id for the html element containing the graph.
        className (str): The class name for the html element containing the figure and titles.
        figure: An object that can be rendered by plotly as a graph.
        title (str): A title for the figure.
        subtitle (str): A subtitle for the figure.

    Returns:
        html.Div: An html Div component that contains the figure, title and subtitle.
    """
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
