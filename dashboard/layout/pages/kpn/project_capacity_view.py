"""
Capacity view
==========
tab_name: Capaciteit
tab_order: 2
"""

from layout.components.capacity_component import capacity_template


def get_html(client):
    return capacity_template(client)
