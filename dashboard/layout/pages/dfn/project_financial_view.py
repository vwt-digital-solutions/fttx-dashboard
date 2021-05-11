"""
Financial view
==========
tab_name: Financieel
tab_order: 2
"""

from layout.components.financial_template import financial_template


def get_html(client):
    """Function to get html for financial view DFN"""
    return financial_template(client)
