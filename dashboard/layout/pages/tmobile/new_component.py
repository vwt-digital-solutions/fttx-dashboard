from layout.components.figure import figure
import plotly.graph_objects as go


def get_html(value, previous_value, title="", sub_title="", font_color=None):
    fig = go.Figure(
        layout={
            "height": 200,
            "font": {'color': font_color},
            "margin": dict(l=10, r=10, t=60, b=10),
        }
    )
    fig.add_trace(
        go.Indicator(
            delta={'reference': previous_value},
            mode="number+delta",
            value=value,
            title={
                "text": f"{title}<br><span style='font-size:0.8em; font-color:light-gray'>{sub_title}</span>"},

        )
    )
    return figure(figure=fig)
