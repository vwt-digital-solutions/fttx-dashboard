import plotly.graph_objects as go

from layout.components.figure import figure

from config import colors_vwt as colors


def get_html(df, title=""):
    if df is not None and not df.empty:
        phase_labels = ['Schouwen', 'BIS', 'Montage-LasAP', 'Montage-LasDP', 'HAS']
        fig = go.Figure(data=[
            go.Bar(name='Opgeleverd HC',
                   x=phase_labels,
                   y=df[df.status == "opgeleverd"]['count'].values,
                   marker=dict(color=colors['green'])
                   ),
            go.Bar(name='Opgeleverd zonder HC',
                   x=phase_labels,
                   y=df[df.status == "opgeleverd_zonder_hc"]['count'].values,
                   marker=dict(color=colors['yellow'])
                   ),
            go.Bar(name='Ingeplanned',
                   x=phase_labels,
                   y=df[df.status == "ingeplanned"]['count'].values,
                   marker=dict(color=colors['orange'])
                   ),
            go.Bar(name='Niet opgeleverd',
                   x=phase_labels,
                   y=df[df.status == "niet_opgeleverd"]['count'].values,
                   marker=dict(color=colors['red'])
                   )
        ])
        fig.update_layout(barmode='stack',
                          title=title,
                          yaxis_title="Aantal woningen")
        return figure(figure=fig)
    return None
