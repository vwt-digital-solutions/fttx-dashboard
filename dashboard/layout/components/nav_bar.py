from config_pages import config_pages
import dash_bootstrap_components as dbc
import config

colors = config.colors_vwt


def nav_bar(huidige_pagina, brand):

    for page in config_pages:
        if huidige_pagina in config_pages[page]['link']:
            huidige_pagina = config_pages[page]['name']
            break

    dropdown_items = []
    for page in config_pages:
        dropdown_items = dropdown_items + [
            dbc.DropdownMenuItem(
                config_pages[page]['name'],
                href=config_pages[page]['link'][0],
                style={'font-size': '1.5rem',
                       'color': colors['vwt_blue']}
            ),
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
            style={'font-size': '1.5rem',
                   'color': colors['vwt_blue']}
        )
    ]

    return dbc.NavbarSimple(
        id='navbar',
        children=children,
        brand=brand,
        sticky='top',
        dark=True,
        color=colors['vwt_blue'],
        style={
            'top': 0,
            'left': 0,
            'position': 'fixed',
            'width': '100%'
        }
    )
