import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc

from config import upload_config


def get_html():
    return html.Div([
        dcc.Dropdown(
            id='upload-validator',
            options=[
                {'label': value['name'], 'value': key} for key, value in upload_config.items()
            ],
            placeholder="Selecteer een upload type",
        ),
        dcc.Upload(
            id='upload-data',
            children=html.Div([
                'Sleep of ',
                html.A('Selecteer bestanden')
            ]),
            style={
                'width': '100%',
                'height': '60px',
                'lineHeight': '60px',
                'borderWidth': '1px',
                'borderStyle': 'dashed',
                'borderRadius': '5px',
                'textAlign': 'center',
                'margin': '10px 0'
            },
            # Allow multiple files to be uploaded
            multiple=False
        ),
        html.Div(id='output-data-upload'),
        dbc.Button("Verzenden", id="upload-submit", className="ml-auto"),
        dcc.Loading(
            html.Div(id='submit-result'),
        )
    ])
