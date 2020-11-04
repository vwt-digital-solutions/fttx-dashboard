import plotly.graph_objects as go

from layout.components.figure import figure
from config import colors_vwt as colors


def indicator(value, previous_value=None, title="", sub_title="", font_color=None, id="", invert_delta=False, **kwargs):
    fig = go.Figure(
        layout={
            "height": 200,
            "font": {'color': font_color},
            "margin": dict(l=10, r=10, t=60, b=20),
            'plot_bgcolor': colors['plot_bgcolor'],
            'paper_bgcolor': colors['paper_bgcolor'],
        }
    )
    indicator_args = dict(
        value=value,
        title={
                "text": f"{title}<br><span style='font-size:0.8em; font-color:light-gray'>{sub_title}</span>"},
        mode='number'
    )

    if previous_value is not None:
        indicator_args['delta'] = {'reference': previous_value}
        if invert_delta:
            indicator_args['delta'].update({
                'increasing.color': 'red',
                'decreasing.color': 'green'
            })
        indicator_args['mode'] += '+delta'

    figure_kwargs = {}
    if kwargs.get('gauge'):
        indicator_args['gauge'] = kwargs['gauge']
        indicator_args['mode'] += '+gauge'
        if kwargs['gauge'].get("shape") == "bullet":
            fig.update_layout(height=150)
            figure_kwargs.update(dict(
                title=title,
                subtitle=sub_title
            ))

    fig.add_trace(
        go.Indicator(**indicator_args)
    )
    return figure(figure=fig, container_id=id, **figure_kwargs)
