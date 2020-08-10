import dash_core_components as dcc
import dash_html_components as html

from layout import default
from app import app
from dash.dependencies import Input, Output
from config_pages import config_pages
from pages import error
from callbacks import *  # noqa: F403, F401

app.layout = html.Div([
    dcc.Location(id='url', refresh=True),
    html.Div(id='page-content')
])


# CALBACKS
@app.callback(
    Output('page-content', 'children'),
    [
        Input('url', 'pathname')
    ]
)
def display_page(pathname):
    body = error
    layout = default.get_layout

    for page in config_pages:
        if pathname in config_pages[page]['link']:
            body = config_pages[page]['body']
            break

    return [layout(pathname=pathname, brand="FttX", children=body.get_body())]


if __name__ == "__main__":
    app.run_server(debug=True)
