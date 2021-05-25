import dash_bootstrap_components as dbc
import dash_html_components as html

from app import app
from data.graph import update_date


def header(header_text=""):
    update_dates = update_date()
    data_processed_operational = update_dates[0]
    data_updated_operational = update_dates[1]
    data_process_finance = update_dates[2]
    data_updated_finance = update_dates[3]
    data_update_text_1 = f"""
Operationele data (Fiberconnect) is binnengekomen op {data_updated_operational}
    , en voor het laatst meegenomen in de analyse op {data_processed_operational}.
    """
    data_update_text_2 = f"""
Financiele data (BAAN) is binnengekomen op {data_updated_finance},
en voor het laatst meegenomen in de analyse op {data_process_finance}.
"""

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
                            "margin-left": "115px",
                        },
                    ),
                ],
                className="one-third column",
                style={"textAlign": "left"},
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.H3(
                                header_text,
                                style={"margin-bottom": "0px", "margin-left": "75px"},
                            ),
                            html.P(
                                id="date_update",
                                children="   Data updated",
                                style={"margin-bottom": "0px", "margin-left": "75px"},
                                className="fa fa-info-circle",
                            ),
                            dbc.Tooltip(
                                children=[
                                    html.P(data_update_text_1),
                                    html.Br(),
                                    html.P(data_update_text_2),
                                ],
                                id="hover",
                                target="date_update",
                                placement="below",
                                style={"font-size": 12},
                            ),
                        ],
                    )
                ],
                className="one-third column",
                id="title",
                style={"textAlign": "center"},
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
                            "margin-right": "0px",
                        },
                    )
                ],
                className="one-third column",
                style={"textAlign": "right"},
            ),
        ],
        id="header",
        className="row",
        style={"margin-bottom": "25px"},
    )
