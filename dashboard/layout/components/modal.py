import dash_bootstrap_components as dbc
from app import app
from dash.dependencies import Input, Output, State


def create_modal(modal_body, modal_id, input_id, modal_title=""):
    print(f"create modal for {modal_id}, {input_id}")
    if f"..{modal_id}.is_open.." not in app.callback_map:
        @app.callback(
            [
                Output(modal_id, "is_open"),
            ],
            [
                Input(f"{modal_id}-close", "n_clicks"),
                Input(input_id, "n_clicks"),
            ],
            [
                State(modal_id, "is_open"),
            ]
        )
        def modal_callback(close_clicks, input_clicks, is_open):
            print("Modal clicked")
            if close_clicks or input_clicks:
                return [not is_open]
            return [is_open]

    def get_html(modal_title, modal_body, modal_id):
        return dbc.Modal(
            [
                dbc.ModalHeader(modal_title),
                dbc.ModalBody(
                    modal_body
                ),
                dbc.ModalFooter(
                    dbc.Button("Close", id=f"{modal_id}-close", className="ml-auto")
                ),
            ],
            id=modal_id,
            size="lg",
            centered=True,
        )
    return get_html(modal_title, modal_body, modal_id)
