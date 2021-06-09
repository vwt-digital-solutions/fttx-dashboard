from config import colors_vwt as colors


def get_fig(data):
    fig = {"data": [], "layout": {}}

    afsluitindicator = {
        "x": list(data["afsluit_indicator"].index),
        "y": list(data["afsluit_indicator"].values),
        "mode": "markers",
        "line": dict(color=colors["green"]),
        "name": "Gerealiseerde aansluitingen in BP",
    }

    planningindicator = {
        "x": list(data["planned_indicator"].index),
        "y": list(data["planned_indicator"].values),
        "mode": "lines",
        "line": dict(color=colors["red"]),
        "name": "Geplande aansluitingen in BP",
    }

    fig["data"] = [afsluitindicator, planningindicator]

    fig["layout"] = {
        "xaxis": {
            "title": "Aansluitdatum [d]",
            # "range": [data['prognose'].index.iloc[0],
            #           data['prognose'].index.iloc[-1]],
        },
        "yaxis": {"title": "Aantal aansluitingen"},  # "range": [0, 110]},
        "title": {"text": "Voortgang Activatie"},
        "showlegend": True,
        "legend": {"x": 0, "xanchor": "left", "y": 1.15},
        "height": 450,
        "plot_bgcolor": colors["plot_bgcolor"],
        "paper_bgcolor": colors["paper_bgcolor"],
    }

    return fig
