from layout.components.figure import figure
import datetime
import pandas as pd


def get_html_overview(data):
    x_count = list(range(1, len(data.date) + 1))
    if data.period.iloc[0] == 'month':
        n_now = datetime.date.today().month - 1
        x_tick_text = ['jan', 'feb', 'maa', 'apr', 'mei', 'jun', 'jul', 'aug', 'sep', 'okt', 'nov', 'dec']
        x_range = [0.5, 12.5]
        title = "Jaaroverzicht"
        width = 0.2
        if data.provider.iloc[0] == 't-mobile':
            y_range = [0, 2000]
        if data.provider.iloc[0] == 'kpn':
            y_range = [0, 18000]
    if data.period.iloc[0] == 'week':
        print(data)
        n_now = int((pd.Timestamp.now() - pd.to_datetime('2019-12-30')).days / 7)
        n_d = int((pd.Timestamp.now() - pd.to_datetime('2020-' + str(datetime.date.today().month) + '-01')).days / 7)
        x_tick_text = [el2 + '<br>W' + str(el + 1) for el, el2 in zip(x_count, data.date.dt.strftime('%Y-%m-%d').to_list())]
        x_range = [n_now - n_d - 0.5, n_now + 4.5 - n_d]
        title = "Maandoverzicht"
        width = 0.08
        if data.provider.iloc[0] == 't-mobile':
            y_range = [0, 600]
        if data.provider.iloc[0] == 'kpn':
            y_range = [0, 3000]

    return figure(
        figure=dict(
            data=[
                dict(name="Voorspelling", x=x_count, y=data.count_voorspellingdatum,
                     mode='markers', marker=dict(color='rgb(200, 200, 0)', symbol='diamond', size=15)),
                dict(name="Planning HPend", x=x_count, y=data.count_hasdatum,
                     type='lines', marker=dict(color='rgb(200, 0, 0)'), width=width),
                dict(name="Realisatie", x=[el + 0.1 for el in x_count], y=data.count_opleverdatum,
                     type='bar', marker=dict(color='rgb(0, 200, 0)'), width=width),
                dict(name="Outlook", x=[el - 0.1 for el in x_count], y=data.count_outlookdatum,
                     type='bar', marker=dict(color='rgb(170, 170, 170)'), width=width),
                dict(name="Huidige week", x=[n_now], y=[y_range[1]],
                     type='bar', marker=dict(color='rgb(0, 0, 0)'), width=width*0.5),
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
    )
