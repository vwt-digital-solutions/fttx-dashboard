import plotly.graph_objects as go

from layout.components.figure import figure


def indicator(value, previous_value=None, title="", sub_title="", font_color=None, id=""):
    fig = go.Figure(
        layout={
            "height": 200,
            "font": {'color': font_color},
            "margin": dict(l=10, r=10, t=60, b=10),
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
        indicator_args['mode'] += '+delta'

    fig.add_trace(
        go.Indicator(**indicator_args)
    )
    return figure(figure=fig, container_id=id)
