import logging

logging.basicConfig(format='%(asctime)s - %(name)s -%(levelname)s - %(filename)s:%(funcName)s:%(lineno)s - %(message)s',
                    level=logging.INFO)


logging.basicConfig(
    format=' %(asctime)s - %(name)s -%(levelname)s - %(filename)s:%(funcName)s:%(lineno)s - %(message)s',
    level=logging.INFO)

logging.info("Importing Dash components")
import dash_core_components as dcc  # noqa: E402
import dash_html_components as html  # noqa: E402
from dash.dependencies import Input, Output  # noqa: E402

logging.info("Importing App")
from app import app  # noqa: E402

logging.info("Imporing callbacks")
from callbacks import dash  # noqa: F403, F401, E402

logging.info("Importing the rest")
from layout import default  # noqa: E402
from config_pages import config_pages  # noqa: E402
from layout.pages import error  # noqa: E402
import importlib  # noqa: E402


logging.info("Setting base layout")
app.layout = html.Div([
    dcc.Location(id='url', refresh=True),
    html.Div(id='page-content')
])


def get_page(page):
    logging.info(f"Getting page {page}")
    try:
        page_body = importlib.import_module(f"layout.pages.{page}.{page}")
    except ImportError:
        page_body = importlib.import_module(f"layout.pages.{page}")
    logging.info("Returning page")
    return page_body


# CALBACKS
@app.callback(
    Output('page-content', 'children'),
    [
        Input('url', 'pathname')
    ]
)
def display_page(pathname):
    ctx = dash.callback_context
    print(ctx.triggered)
    logging.info(f"Display page {pathname}")
    page_body = error
    layout = default.get_layout

    for page in config_pages:
        if pathname in config_pages[page]['link']:
            page_body = get_page(page)
            break

    return [layout(pathname=pathname, brand="FttX", children=page_body.get_body())]


if __name__ == "__main__":
    logging.info("Starting server")
    app.run_server(debug=True)
