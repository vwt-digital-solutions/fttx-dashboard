import plotly.graph_objects as go

from config import colors_vwt as colors
from layout.components.figure import figure


def indicator(
    value,
    value2=0,
    previous_value=None,
    title="",
    sub_title="",
    font_color=None,
    id="",
    invert_delta=False,
    percentage=False,
    suffix=None,
    gauge=None,
    gauge_type=None,
    **kwargs,
):
    fig = go.Figure(
        layout={
            "height": 200,
            "font": {"color": font_color},
            "margin": dict(l=10, r=10, t=40, b=20),
            "plot_bgcolor": colors["plot_bgcolor"],
            "paper_bgcolor": colors["paper_bgcolor"],
        }
    )
    indicator_args = dict(
        value=value,
        number=dict(valueformat=".2%") if percentage else dict(valueformat=":"),
        # title={
        #     "text": f"{title}<br><span style='font-size:0.8em; font-color:light-gray'>{sub_title}</span>"
        # },
        mode="number",
    )

    if suffix:
        indicator_args["number"]["suffix"] = suffix

    if previous_value is not None:
        indicator_args["delta"] = {"reference": previous_value, "valueformat": ":"}
        if invert_delta:
            indicator_args["delta"].update(
                {"increasing.color": "red", "decreasing.color": "green"}
            )
        indicator_args["mode"] += "+delta"

    figure_kwargs = {}

    if kwargs.get("gauge"):
        indicator_args["gauge"] = kwargs.get("gauge")
        indicator_args["mode"] += "+gauge"
    elif gauge_type == "bullet":
        max_value = int(max(value, value2, 1) * 1.1)
        indicator_args["gauge"] = {
            "shape": "bullet",
            "axis": {"range": [0, max_value]},
            "threshold": {
                "line": {"color": "red", "width": 2},
                "thickness": 0.3,
                "value": max(value2, 0.01),
            },
            "steps": [
                {"range": [0, int(value2 * 0.9)], "color": "yellow"},
                {"range": [int(value2 * 0.9), max_value], "color": "lightgreen"},
            ],
        }
        indicator_args["mode"] += "+gauge"
        fig.update_layout(height=150)
        figure_kwargs.update(dict(title=title, subtitle=sub_title + str(value2)))
    elif gauge_type == "speedo":
        indicator_args["gauge"] = {
            "axis": {"range": [None, 1], "tickwidth": 1, "tickcolor": "green"},
            "bar": {"color": "darkgreen"},
            "bgcolor": "white",
            "borderwidth": 2,
            "bordercolor": "gray",
            "steps": [
                {"range": [0, 0.6], "color": "yellow"},
                {"range": [0.6, 1], "color": "lightgreen"},
            ],
            "threshold": {
                "line": {"color": "red", "width": 4},
                "thickness": 0.75,
                "value": 0.9,
            },
        }
        indicator_args["mode"] += "+gauge"
        indicator_args["title"] = {
            "text": f"{title}<br><span style='font-size:0.8em; font-color:light-gray'>{sub_title + str(value2)}</span>"
        }
        fig.update_layout(height=250)

    fig.add_trace(go.Indicator(**indicator_args))
    return figure(figure=fig, container_id=id, **figure_kwargs)
