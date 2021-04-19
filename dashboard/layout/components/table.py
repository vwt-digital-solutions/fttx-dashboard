import dash_html_components as html


def table(
    container_id: str = "",
    table_id: str = "",
    className: str = "pretty_container column",
    table=None,
    title: str = "",
    subtitle: str = "",
):

    return html.Div(
        [
            html.H5(title, id=f"{container_id}-title"),
            html.H6(subtitle, id=f"{container_id}-subtitle"),
            html.Div(id=table_id, children=table),
        ],
        className=className,
        hidden=False,
        id=container_id,
    )
