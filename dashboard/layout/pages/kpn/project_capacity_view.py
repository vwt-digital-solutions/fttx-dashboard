"""
Capacity view
==========
tab_name: Capaciteit
tab_order: 2
"""
from app import toggles
from layout.components.capacity_component import capacity_template

if toggles.capacity_view:
    def get_html(client):
        return capacity_template(client)
