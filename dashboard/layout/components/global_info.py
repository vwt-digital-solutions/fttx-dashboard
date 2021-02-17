import dash_html_components as html


def global_info(title="",
                id_="",
                text1="",
                value1="",
                text2="",
                value2="",
                className="pretty_container column"):
    return html.Div(
        [
            html.P([html.Strong(title)]),
            html.P(id=id_ + "_text", children=text1 + value1),
            html.P(id=id_ + "_text", children=text2 + value2)
        ],
        id=id_,
        className=className,
        hidden=False,
    )
