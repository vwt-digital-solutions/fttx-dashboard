from collections import OrderedDict
from layout.pages import main_page, tmobile_page, kpn_page

config_pages = OrderedDict(
    [
        ('main_page', {
            'name': 'Projecten',
            'link': ["/", '/main_page', '/main_page/'],
            'body': main_page
        }),
        ('tmobile', {
            'name': 'T-Mobile',
            'link': ['/tmobile', '/tmobile/'],
            'body': tmobile_page
        }),
        ('kpn', {
            'name': 'KPN',
            'link': ['/kpn', '/kpn/'],
            'body': kpn_page
        })
    ]
)
