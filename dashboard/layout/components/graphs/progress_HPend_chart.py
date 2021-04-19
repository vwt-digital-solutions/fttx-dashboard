from config import colors_vwt as colors


def get_fig(data):
    fig = {"data": [], "layout": {}}

    if not data["prognose"].empty:
        fig["data"] = fig["data"] + [
            {
                "x": list(data["prognose"].index),
                "y": list(data["prognose"].values),
                "mode": "lines",
                "line": dict(color=colors["yellow"]),
                "name": "Voorspelling",
            }
        ]

    if not data["realisatie"].empty:
        fig["data"] = fig["data"] + [
            {
                "x": list(data["realisatie"].index),
                "y": list(data["realisatie"].values),
                "mode": "markers",
                "line": dict(color=colors["green"]),
                "name": "Realisatie HAS",
            }
        ]

    if not data["target"].empty:
        fig["data"] = fig["data"] + [
            {
                "x": list(data["target"].index),
                "y": list(data["target"].values),
                "mode": "lines",
                "line": dict(color=colors["lightgray"]),
                "name": "Internal Target",
            }
        ]

    fig["layout"] = {
        "xaxis": {
            "title": "Opleverdatum [d]",
            # "range": [data['prognose'].index.iloc[0],
            #           data['prognose'].index.iloc[-1]],
        },
        # "yaxis": {"title": "Opgeleverd HPend [%]", "range": [0, 110]},
        "title": {"text": "Voortgang project vs internal target:"},
        "showlegend": True,
        "legend": {"x": 0, "xanchor": "left", "y": 1.15},
        "height": 450,
        "plot_bgcolor": colors["plot_bgcolor"],
        "paper_bgcolor": colors["paper_bgcolor"],
    }

    return fig
