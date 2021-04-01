from config import colors_vwt as colors
from layout.components.graphs import pie_chart


def get_fig(data, title):
    fig = pie_chart.get_html(
        labels=list(data.keys()),
        values=list(data.values()),
        title=title,
        colors=[
            colors["green"],
            colors["yellow"],
            colors["red"],
            colors["vwt_blue"],
        ],
    )
    return fig
