from config import colors_vwt as colors


def get_fig(data):
    x = data["x"]
    y = data["y"]
    names = data["names"]

    x_min = -30
    x_max = 30  # + max([abs(min(x)), abs(max(x))])
    y_min = -30
    y_max = 250  # + max([abs(min(y)), abs(max(y))])
    y_voorraad_p = 90
    fig = {
        "data": [
            {
                "x": [x_min, 1 / 70 * x_min, 1 / 70 * x_min, x_min],
                "y": [y_min, y_min, y_voorraad_p, y_voorraad_p],
                "name": "Trace 2",
                "mode": "lines",
                "fill": "toself",
                "opacity": 1,
                "line": {"color": colors["red"]},
            },
            {
                "x": [
                    1 / 70 * x_min,
                    1 / 70 * x_max,
                    1 / 70 * x_max,
                    15,
                    15,
                    1 / 70 * x_min,
                ],
                "y": [y_min, y_min, y_voorraad_p, y_voorraad_p, 150, 150],
                "name": "Trace 2",
                "mode": "lines",
                "fill": "toself",
                "opacity": 1,
                "line": {"color": colors["green"]},
            },
            {
                "x": [
                    x_min,
                    1 / 70 * x_min,
                    1 / 70 * x_min,
                    15,
                    15,
                    1 / 70 * x_max,
                    1 / 70 * x_max,
                    x_max,
                    x_max,
                    x_min,
                    x_min,
                    1 / 70 * x_min,
                ],
                "y": [
                    y_voorraad_p,
                    y_voorraad_p,
                    150,
                    150,
                    y_voorraad_p,
                    y_voorraad_p,
                    y_min,
                    y_min,
                    y_max,
                    y_max,
                    y_voorraad_p,
                    y_voorraad_p,
                ],
                "name": "Trace 2",
                "mode": "lines",
                "fill": "toself",
                "opacity": 1,
                "line": {"color": colors["yellow"]},
            },
            {
                "x": x,
                "y": y,
                "text": names,
                "name": "Trace 1",
                "mode": "markers",
                "marker": {"size": 15, "color": colors["black"]},
            },
        ],
        "layout": {
            "clickmode": "event+select",
            "xaxis": {
                "title": "Procent voor of achter HPEnd op Internal Target",
                "range": [x_min, x_max],
                "zeroline": False,
            },
            "yaxis": {
                "title": "Procent voor of achter op verwachte werkvoorraad",
                "range": [y_min, y_max],
                "zeroline": False,
            },
            "showlegend": False,
            "title": {
                "text": "Krijg alle projecten in het groene vlak door de pijlen te volgen"
            },
            "annotations": [
                dict(
                    x=-12.5,
                    y=25,
                    ax=0,
                    ay=40,
                    xref="x",
                    yref="y",
                    text="Verhoog schouw of BIS capaciteit",
                    alignment="left",
                    showarrow=True,
                    arrowhead=2,
                )
            ]
            + [
                dict(
                    x=12.5,
                    y=25,
                    ax=0,
                    ay=40,
                    xref="x",
                    yref="y",
                    text="Verhoog schouw of BIS capaciteit",
                    alignment="left",
                    showarrow=True,
                    arrowhead=2,
                )
            ]
            + [
                dict(
                    x=-13.5,
                    y=160,
                    ax=-100,
                    ay=0,
                    xref="x",
                    yref="y",
                    text="Verhoog HAS capaciteit",
                    alignment="left",
                    showarrow=True,
                    arrowhead=2,
                )
            ]
            + [
                dict(
                    x=-13.5,
                    y=40,
                    ax=-100,
                    ay=0,
                    xref="x",
                    yref="y",
                    text="Verruim klantafspraak",
                    alignment="left",
                    showarrow=True,
                    arrowhead=2,
                )
            ]
            + [
                dict(
                    x=13.5,
                    y=160,
                    ax=100,
                    ay=0,
                    xref="x",
                    yref="y",
                    text="Verlaag HAS capcaciteit",
                    alignment="right",
                    showarrow=True,
                    arrowhead=2,
                )
            ]
            + [
                dict(
                    x=13.5,
                    y=40,
                    ax=100,
                    ay=0,
                    xref="x",
                    yref="y",
                    text="Verscherp klantafspraak",
                    alignment="right",
                    showarrow=True,
                    arrowhead=2,
                )
            ]
            + [
                dict(
                    x=12.5,
                    y=185,
                    ax=0,
                    ay=-40,
                    xref="x",
                    yref="y",
                    text="Verlaag schouw of BIS capaciteit",
                    alignment="left",
                    showarrow=True,
                    arrowhead=2,
                )
            ]
            + [
                dict(
                    x=-12.5,
                    y=185,
                    ax=0,
                    ay=-40,
                    xref="x",
                    yref="y",
                    text="Verlaag schouw of BIS capaciteit",
                    alignment="left",
                    showarrow=True,
                    arrowhead=2,
                )
            ],
            "margin": {"l": 60, "r": 15, "b": 40, "t": 40},
            "plot_bgcolor": colors["plot_bgcolor"],
            "paper_bgcolor": colors["paper_bgcolor"],
        },
    }
    return fig
