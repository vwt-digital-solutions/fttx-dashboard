import dash_html_components as html


def global_info_old(id_="", title="", text="", value="", className="pretty_container column"):
    return html.Div(
        [
            html.P([html.Strong(title)]),
            html.P(id=id_ + "_text", children=text + value)
        ],
        id=id_,
        className=className,
        hidden=False,
    )
