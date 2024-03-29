from config import colors_vwt as colors


def get_fig(data, unit_type):
    color_key = {
        "Target": "vwt_blue",
        "Gerealiseerd & ideaal verloop": "lightgray",
        "Gerealiseerd & verwacht verloop": "darkgray",
        "Totale productie voorafgaande fase": "black",
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

    if unit_type == "_units":
        y_title = "Aantal"
    else:
        y_title = "%"

    fig["layout"] = {
        "yaxis": {"title": y_title},  # , "range": [0, 110]},
        "showlegend": True,
        "height": 500,
        "plot_bgcolor": colors["plot_bgcolor"],
        "paper_bgcolor": colors["paper_bgcolor"],
    }

    return fig
