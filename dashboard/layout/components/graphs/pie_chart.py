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

    fig.update_layout(
        title_text=title,
        plot_bgcolor=config.colors_vwt['plot_bgcolor'],
        paper_bgcolor=config.colors_vwt['paper_bgcolor'],
    )
    return fig
