from collections import OrderedDict

config_pages = OrderedDict(
    [
        ('main_page', {
            'name': 'Overzicht Projecten',
            'link': ["/", '/main_page', '/main_page/'],
        }),
        ('tmobile', {
            'name': 'T-Mobile',
            'link': ['/tmobile', '/tmobile/'],
        }),
        ('kpn', {
            'name': 'KPN',
            'link': ['/kpn', '/kpn/'],
        })
    ]
)
