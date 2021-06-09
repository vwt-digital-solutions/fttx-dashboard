from plotly import graph_objects as go

import config


def get_fig(bar):
    fig = go.Figure(
        [
            go.Bar(
                name=bar.get('name'),
                x=bar.get("x"),
                y=bar.get("y"),
                marker_color=bar.get('color'),
                orientation='h',
                text=bar[bar.get('text')] if 'text' in bar else '',
                textposition='outside'
            )
        ]
    )
    fig.update_layout(
        height=500,
        paper_bgcolor=config.colors_vwt['paper_bgcolor'],
        plot_bgcolor=config.colors_vwt['plot_bgcolor'],
        title=bar.get('title') if 'title' in bar else ''
    )
    return fig
