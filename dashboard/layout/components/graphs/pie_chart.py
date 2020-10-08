import plotly.graph_objects as go

import config


def get_html(labels, values, title="", colors=None):
    if not colors:
        colors = list(config.colors_vwt.values())

    fig = go.Figure(
        data=[
            go.Pie(labels=labels,
                   values=values,
                   marker_colors=colors)
        ]
    )

    update_traces = {
        'hoverinfo': 'label+percent',
        'textinfo': 'value'
    }

    if colors:
        update_traces["marker"] = dict(colors=colors)
    fig.update_traces(**update_traces)

    fig.update_layout(
        title_text=title
    )
    return fig
