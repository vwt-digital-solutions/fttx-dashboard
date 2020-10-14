import pandas as pd

from config import colors_vwt as colors


def get_fig(data):
    x_count = list(range(1, len(data.date) + 1))
    y_range = [0, 2.2 * data.count_opleverdatum.max()]
    date_list = data.date.dt.strftime("%Y-%m-%d")

    if data.period.iloc[0] == 'month':
        n_now = pd.Timestamp.now().month
        x_tick_text = ['jan', 'feb', 'maa', 'apr', 'mei', 'jun', 'jul', 'aug', 'sep', 'okt', 'nov', 'dec']
        x_range = [0.5, 12.5]
        title = "Jaaroverzicht"
        width = 0.2

    if data.period.iloc[0] == 'week':
        n_now = pd.Timestamp.now().weekofyear - 1
        x_tick_text = [el2 + '<br>W' + str(el + 1) for el, el2 in
                       zip(x_count, data.date.dt.strftime('%Y-%m-%d').to_list())]
        x_range = [n_now - 1.5, n_now + 3.5]
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
                 type='bar', marker=dict(color=colors['black']), width=width * 0.2),
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
            'plot_bgcolor': colors['plot_bgcolor'],
            'paper_bgcolor': colors['paper_bgcolor'],
            'hovermode': 'closest'
        }
    )