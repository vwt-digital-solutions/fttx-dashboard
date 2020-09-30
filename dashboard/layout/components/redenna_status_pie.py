import plotly.graph_objects as go

import config


def get_fig(redenna_counts, title="", colors=None):
    fig = go.Figure()

    if not colors:
        colors = list(config.colors_vwt.values())

    fig.add_trace(
        go.Pie(labels=redenna_counts.total.cluster_redenna,
               values=redenna_counts.total['count'],
               marker_colors=colors,
               visible=True)
    )

    fig.add_trace(
        go.Pie(labels=redenna_counts.laagbouw.cluster_redenna,
               values=redenna_counts.laagbouw['count'],
               marker_colors=colors,
               visible=False),
    )

    fig.add_trace(
        go.Pie(labels=redenna_counts.hoogbouw.cluster_redenna,
               values=redenna_counts.hoogbouw['count'],
               marker_colors=colors,
               visible=False),
    )

    fig.update_layout(
        updatemenus=[
            dict(
                type="dropdown",
                direction="down",
                active=0,
                x=0.3,
                xanchor="right",
                y=1.1,
                yanchor="top",
                buttons=list([
                    dict(label="Totaal",
                         method="update",
                         args=[{"visible": [True, False, False]}]),
                    dict(label="Laagbouw",
                         method="update",
                         args=[{"visible": [False, True, False]}]),
                    dict(label="Hoogbouw",
                         method="update",
                         args=[{"visible": [False, False, True]}]),
                ]),
            )
        ])

    fig.update_layout(
        title_text=title,
        height=500,
        margin=dict(l=50, r=0, t=100, b=0)
    )

    return fig
