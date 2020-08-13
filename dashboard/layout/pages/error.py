import dash_html_components as html
from flask import Response


def get_body():
    Response('Not Found', 404)
    return html.Div(["""deze pagina bestaat niet, druk op vorige
                   of een van de paginas in het menu hierboven"""])
