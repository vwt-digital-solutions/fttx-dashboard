import dash_html_components as html

from layout.components.global_info_old import global_info_old


def global_info_list_old(items, **kwargs):
    return html.Div([global_info_old(**item) for item in items], **kwargs)
