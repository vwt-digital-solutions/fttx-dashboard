import dash_html_components as html

from components.nav_bar import nav_bar


def get_layout(pathname="/", brand="", children=None):
    layout = html.Div(
        [
            nav_bar(pathname, brand),
            html.Div(children)
        ]
    )
    return layout
