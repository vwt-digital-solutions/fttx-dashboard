import dash_html_components as html

from data.graph import update_date

from app import app


def header(header_text=""):
    date_updated = update_date()[1]
    date_processed = update_date()[0]
    data_update_text = f"Data binnengekomen op {date_updated}, data laatst verwerkt op {date_processed}"

    return html.Div(
        [
            html.Div(
                [
                    html.Img(
                        src=app.get_asset_url("ODH_logo_original.png"),
                        id="DAT-logo",
                        style={
                            "height": "70px",
                            "width": "auto",
                            "margin-bottom": "15px",
                            "margin-left": "115px"
                        },
                    ),
                ],
                className="one-third column",
                style={'textAlign': 'left'}
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.H3(
                                header_text,
                                style={"margin-bottom": "0px", "margin-left": "75px"},
                            ),
                            html.P(id='date_update',
                                   children=data_update_text,
                                   style={"margin-bottom": "0px", "margin-left": "75px"},
                                   )
                        ],
                    )
                ],
                className="one-third column",
                id="title",
                style={'textAlign': 'center'}
            ),
            html.Div(
                [
                    html.Img(
                        src=app.get_asset_url("vqd.png"),
                        id="vqd-image",
                        style={
                            "height": "100px",
                            "width": "auto",
                            "margin-bottom": "15px",
                            "margin-right": "0px"
                        },
                    )
                ],
                className="one-third column",
                style={'textAlign': 'right'}
            ),
        ],
        id="header",
        className="row",
        style={"margin-bottom": "25px"},
    )
