import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
import main_page

from app import app
from collections import OrderedDict
from dash.dependencies import Input, Output

config_pages = OrderedDict(
    [
        ('main_page', {
            'name': 'Projecten',
            'link': ['/main_page', '/main_page/'],
            'body': main_page
        }),
    ]
)


# NAVBAR
def get_navbar(huidige_pagina):

    for page in config_pages:
        if huidige_pagina in config_pages[page]['link']:
            huidige_pagina = config_pages[page]['name']
            break

    dropdown_items = []
    for page in config_pages:
        dropdown_items = dropdown_items + [
            dbc.DropdownMenuItem(config_pages[page]['name'], href=config_pages[page]['link'][0], style={'font-size': '1.5rem'}),
            dbc.DropdownMenuItem(divider=True)
        ]

    dropdown_items = dropdown_items[:-1]

    children = [
        dbc.NavItem(dbc.NavLink(huidige_pagina, href='#')),
        dbc.DropdownMenu(
            nav=True,
            in_navbar=True,
            label='Menu',
            children=dropdown_items,
            style={'font-size': '1.5rem'}
        )
    ]

    return dbc.NavbarSimple(
        children=children,
        brand='FttX',
        sticky='top',
        dark=True,
        color='grey',
        style={
            'top': 0,
            'left': 0,
            'position': 'fixed',
            'width': '100%'
        }
    )


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
    # startpagina
    if pathname == '/':
        return [get_navbar('/main_page'), main_page.get_body()]
    if pathname == '/main_page':
        return [get_navbar(pathname), main_page.get_body()]

    return [get_navbar(pathname), html.P('''deze pagina bestaat niet, druk op vorige
                   of een van de paginas in het menu hierboven''')]


if __name__ == "__main__":
    app.run_server(debug=True)
