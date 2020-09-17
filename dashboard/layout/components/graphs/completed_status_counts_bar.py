import plotly.graph_objects as go

from layout.components.figure import figure


def get_html(df, title=""):
    if df is not None and not df.empty:
        phase_labels = ['Schouwen', 'BIS', 'Montage-LasAP', 'Montage-LasDP', 'HAS']
        fig = go.Figure(
            data=[
                go.Bar(name='Opgeleverd HC',
                       x=phase_labels,
                       y=df[df.status == "opgeleverd"]['count'].values,
                       marker=dict(color='rgb(0, 200, 0)')
                       ),
                go.Bar(name='Opgeleverd zonder HC',
                       x=phase_labels,
                       y=df[df.status == "opgeleverd_zonder_hc"]['count'].values,
                       marker=dict(color='rgb(200, 200, 0)')
                       ),
                go.Bar(name='Niet opgeleverd',
                       x=phase_labels,
                       y=df[df.status == "niet_opgeleverd"]['count'].values,
                       marker=dict(color='rgb(200, 0, 0)')
                       )
            ]
        )
        fig.update_layout(barmode='stack',
                          title=title,
                          yaxis_title="Aantal woningen")
        return figure(figure=fig)
    return None
