import dash_html_components as html

from layout.components.global_info import global_info


def global_info_list(items, **kwargs):
    return html.Div([global_info(**item) for item in items], **kwargs)
