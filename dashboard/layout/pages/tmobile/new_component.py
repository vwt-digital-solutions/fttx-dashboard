import plotly.graph_objects as go
import datetime
import pandas as pd
from layout.components.figure import figure
from config import colors_vwt as colors


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


def get_html_overview(data):
    x_count = list(range(1, len(data.date) + 1))
    y_range = [0, 1.2 * data.count_hasdatum.max()]
    date_list = data.date.dt.strftime("%Y-%m-%d")

    if data.period.iloc[0] == 'month':
        n_now = datetime.date.today().month - 1
        x_tick_text = ['jan', 'feb', 'maa', 'apr', 'mei', 'jun', 'jul', 'aug', 'sep', 'okt', 'nov', 'dec']
        x_range = [0.5, 12.5]
        title = "Jaaroverzicht"
        width = 0.2

    if data.period.iloc[0] == 'week':
        n_now = int((pd.Timestamp.now() - pd.to_datetime('2019-12-30')).days / 7)
        n_d = int((pd.Timestamp.now() - pd.to_datetime('2020-' + str(datetime.date.today().month) + '-01')).days / 7)
        x_tick_text = [el2 + '<br>W' + str(el + 1) for el, el2 in zip(x_count, data.date.dt.strftime('%Y-%m-%d').to_list())]
        x_range = [n_now - n_d - 0.5, n_now + 4.5 - n_d]
        title = "Maandoverzicht"
        width = 0.08

    return dict(
            data=[
                dict(name="Voorspelling", x=x_count, y=data.count_voorspellingdatum, customdata=date_list,
                     mode='markers', marker=dict(color=colors['yellow'], symbol='diamond', size=15)),
                dict(name="Planning HPend", x=x_count, y=data.count_hasdatum, customdata=date_list,
                     type='lines', marker=dict(color=colors['red']), width=width),
                dict(name="Realisatie", x=[el + 0.5 * width for el in x_count], y=data.count_opleverdatum,
                     customdata=date_list, type='bar', marker=dict(color=colors['green']), width=width),
                dict(name="Outlook", x=[el - 0.5 * width for el in x_count], y=data.count_outlookdatum,
                     customdata=date_list, type='bar', marker=dict(color=colors['lightgray']), width=width),
                dict(name="Huidige week", x=[n_now], y=[y_range[1]], customdata=date_list,
                     type='bar', marker=dict(color=colors['black']), width=width*0.5),
            ],
            layout={
                "barmode": 'stack',
                'showlegend': True,
                'legend': {'orientation': 'h', 'x': -0.075, 'xanchor': 'left', 'y': -0.25, 'font': {'size': 10}},
                'height': 300,
                'margin': {'l': 5, 'r': 15, 'b': 5, 't': 40},
                "title": title,
                'yaxis': {'title': 'Aantal HPend', 'range': y_range},
                'xaxis': {'tickvals': x_count, 'ticktext': x_tick_text, 'range': x_range},
            }
        )
