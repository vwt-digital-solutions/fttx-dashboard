import plotly.graph_objects as go


def indicator(value, previous_value, title=""):
    fig = go.Figure()
    fig.add_trace(
        go.Indicator(
            delta={'reference': previous_value},
            mode="number+delta",
            value=value,
            title={'text': title},
        )
    )
    return fig
