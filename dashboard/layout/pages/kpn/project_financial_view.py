"""
Financial view
==========
tab_name: Financieel
tab_order: 3
"""

from app import toggles
from layout.components.financial_template import financial_template

if toggles.financial_view:
    def get_html(client):
        return financial_template(client)
