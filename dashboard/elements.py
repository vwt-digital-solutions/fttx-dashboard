from config import colors_vwt as colors

site_colors = {
    'black': '#000000',
    'indigo': '#1E22AA',
    'cyan': '#009FDF',
    'grey80': '#575757',
    'grey60': '#878787',
    'grey40': '#B2B2B2',
    'grey20': '#DADADA',
    'white': '#FFFFFF',
    'silver': '#AFA9A0',
}

table_style_header = {
    'backgroundColor': 'white',
    'fontWeight': 'bold',
    'borderBottom': '1px solid black',
    'color': colors['vwt_blue'],
    'align': 'center',
}

table_style_cell_problem = {
    'font-family': 'helvetica',
    'boxShadow': '0 0',
    'backgroundColor': colors['lightgray'],
    'textAlign': 'center',
    'minWidth': '120px',
    'maxWidth': '500px',
}

table_style_cell_action = table_style_cell_problem.copy()
table_style_cell_action['maxWidth'] = '300px'

table_styles = {
    'filter': {
        'font-family': 'helvetica',
        'backgroundColor': colors['lightgray'],
        'textAlign': 'left',
    },
    'header': table_style_header,
    'cell': {
        'problem': table_style_cell_problem,
        'action': table_style_cell_action,
        'conditional': [
            {'if': {'row_index': 'odd'},
                'backgroundColor': 'rgb(256, 256, 256)'},
            {'if': {'row_index': 'even'},
                'backgroundColor': 'rgb(240, 240, 240)'},
        ],
    },
}
