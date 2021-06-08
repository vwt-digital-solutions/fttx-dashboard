"""
Financial view
==========
tab_name: Financieel
tab_order: 4
"""

from layout.components.financial_template import financial_template


def get_html(client):
    """Function to get html for financial view"""
    return financial_template(client)
