import plotly.graph_objects as go


def get_html(labels, values, title="", colors=None):
    fig = go.Figure(
        data=[
            go.Pie(labels=labels,
                   values=values)
        ]
    )

    update_traces = {
        'hoverinfo': 'label+percent',
        'textinfo': 'value'
    }

    if colors:
        update_traces["colors"] = colors
    fig.update_traces(**update_traces)

    fig.update_layout(
        title_text=title
    )
    return fig
