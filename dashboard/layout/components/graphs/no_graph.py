import config


def no_graph(title="", text="No Data"):
    """
    Creates a figure with no data. It accepts a title and a text. The text will be displayed in a large font in place of
    of the graph.

    Args:
        title (str): optional title
        text (str): optional, default: "No Data"

    Returns:
        dict: A dictionary in the plotly figure format.
    """
    return {
        "layout": {
            "paper_bgcolor": config.colors_vwt["paper_bgcolor"],
            "plot_bgcolor": config.colors_vwt["plot_bgcolor"],
            "xaxis": {"visible": False},
            "yaxis": {"visible": False},
            "annotations": [
                {
                    "text": text,
                    "xref": "paper",
                    "yref": "paper",
                    "showarrow": False,
                    "font": {"size": 28},
                }
            ],
            "title": title,
        }
    }
