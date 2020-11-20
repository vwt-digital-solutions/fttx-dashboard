from plotly import graph_objects as go

import config


def get_fig(*bars):
    fig = go.Figure(
        [
            go.Bar(
                name=bar.get('name'),
                x=bar.get("x"),
                y=bar.get("y"),
                marker_color=bar.get('color')
            ) for bar in bars
        ]
    )
    fig.update_layout(
        height=500,
        paper_bgcolor=config.colors_vwt['paper_bgcolor'],
        plot_bgcolor=config.colors_vwt['plot_bgcolor'],
    )
    return fig
