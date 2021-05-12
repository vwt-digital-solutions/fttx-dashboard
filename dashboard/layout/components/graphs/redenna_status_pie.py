import plotly.graph_objects as go
import config


def get_fig(total, laagbouw, hoogbouw, title=""):
    colors = [config.colors_vwt["green"],
              config.colors_vwt["yellow"],
              config.colors_vwt["red"],
              config.colors_vwt["vwt_blue"],
              ]

    fig = go.Figure()
    fig.add_trace(
        go.Pie(labels=total.cluster_redenna,
               values=total['count'],
               marker_colors=colors,
               visible=True)
    )

    fig.add_trace(
        go.Pie(labels=laagbouw.cluster_redenna,
               values=laagbouw['count'],
               marker_colors=colors,
               visible=False),
    )

    fig.add_trace(
        go.Pie(labels=hoogbouw.cluster_redenna,
               values=hoogbouw['count'],
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
        margin=dict(l=50, r=0, t=100, b=0),
        plot_bgcolor=config.colors_vwt['plot_bgcolor'],
        paper_bgcolor=config.colors_vwt['paper_bgcolor'],
    )

    return fig
