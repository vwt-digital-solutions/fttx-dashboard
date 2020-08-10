from collections import OrderedDict

from layout.pages import main_page

config_pages = OrderedDict(
    [
        ('main_page', {
            'name': 'Projecten',
            'link': ["/", '/main_page', '/main_page/'],
            'body': main_page
        }),
    ]
)
