from app import toggles
from layout.components.financial_template import financial_template

if toggles.financial_view:
    def get_html(client):
        return financial_template(client)
