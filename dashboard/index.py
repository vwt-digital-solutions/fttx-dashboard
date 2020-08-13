import dash_core_components as dcc
import dash_html_components as html

from layout import default
from app import app
from dash.dependencies import Input, Output
from config_pages import config_pages
from layout.pages import error
import importlib
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
    page_body = error
    layout = default.get_layout

    for page in config_pages:
        if pathname in config_pages[page]['link']:
            page_body = importlib.import_module(f"layout.pages.{page}")
            break

    return [layout(pathname=pathname, brand="FttX", children=page_body.get_body())]


if __name__ == "__main__":
    app.run_server(debug=True)
