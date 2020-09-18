import plotly.graph_objects as go

from layout.components.figure import figure


def get_html(data, week, graph_id=""):
    fig = go.Figure(
        data=[
            go.Pie(labels=list(data[week].keys()), values=list(data[week].values()))
        ]
    )

    fig.update_traces(hoverinfo='label+percent', textinfo='value')
    return figure(figure=fig, graph_id=graph_id)
