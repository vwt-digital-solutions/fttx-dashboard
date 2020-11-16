import dash_html_components as html

from app import toggles

if toggles.financial_view:
    def get_html(client):
        return html.Div(f"Financien voor {client}")
