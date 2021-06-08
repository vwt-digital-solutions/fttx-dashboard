"""
Capacity view
==========
tab_name: Capaciteit
tab_order: 3
"""
from layout.components.capacity.capacity_component import capacity_template


def get_html(client):
    """Function to get the html for the capacity tab"""
    return capacity_template(client)
