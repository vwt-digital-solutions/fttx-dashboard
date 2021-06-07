from config import colors_vwt as colors


def get_fig(data):
    color_key = {
        "Target": "vwt_blue",
        "Ideaal verloop": "lightgray",
        "Verwacht verloop": "darkgray",
        "Werkvoorraad (totale productie)": "black",
    }
    fig = {"data": [], "layout": {}}

    for key in data:
        fig["data"] = fig["data"] + [
            {
                "x": list(data[key].index),
                "y": list(data[key].values),
                "mode": "lines+markers",
                "line": dict(color=colors[color_key[key]]),
                "name": key,
            }
        ]

    fig["layout"] = {
        "yaxis": {"title": "%"},  # , "range": [0, 110]},
        "showlegend": True,
        "height": 500,
        "plot_bgcolor": colors["plot_bgcolor"],
        "paper_bgcolor": colors["paper_bgcolor"],
    }

    return fig
