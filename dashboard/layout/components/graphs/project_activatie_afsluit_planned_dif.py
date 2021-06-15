import pandas as pd

from config import colors_vwt as colors


def get_fig(data):
    date_list = data["afsluit_indicator"].index.format()
    x_count = list(range(1, len(date_list) + 1))
    y_range = [
        0,
        max(
            sum(data["afsluit_indicator"]),
            sum(data["planned_indicator"]),
        ),
    ]

    dutch_month_list = [
        "jan",
        "feb",
        "maa",
        "apr",
        "mei",
        "jun",
        "jul",
        "aug",
        "sep",
        "okt",
        "nov",
        "dec",
    ]

    n_now = pd.Timestamp.now().month
    maand_of_week = "Huidige maand"
    x_tick_text = dutch_month_list
    x_range = [0.5, 12.5]
    title = "Voortgang Activatie"
    width = 0.2

    output = dict(
        data=[
            dict(
                name="Gepland in BP",
                x=x_count,
                y=data["afsluit_indicator"],
                customdata=date_list,
                type="lines",
                marker=dict(color=colors["red"]),
                width=width,
            ),
            dict(
                name="Gerealiseerd in BP",
                x=[el + 0.5 * width for el in x_count],
                y=data["afsluit_indicator"],
                customdata=date_list,
                type="bar",
                marker=dict(color=colors["green"]),
                width=width,
            ),
            dict(
                name=maand_of_week,
                x=[n_now],
                y=[y_range[1]],
                customdata=date_list,
                type="bar",
                marker=dict(color=colors["black"]),
                width=width * 0.2,
            ),
        ],
        layout={
            "barmode": "stack",
            "showlegend": True,
            "title": title,
            "yaxis": {"title": "Aantal aansluitingen", "range": y_range},
            "xaxis": {"tickvals": x_count, "ticktext": x_tick_text, "range": x_range},
            "showlegend": True,
            "legend": {
                "orientation": "h",
                "x": -0.075,
                "xanchor": "left",
                "y": -0.25,
                "font": {"size": 10},
            },
            "height": 450,
            "plot_bgcolor": colors["plot_bgcolor"],
            "paper_bgcolor": colors["paper_bgcolor"],
            "hovermode": "closest",
        },
    )
    return output
