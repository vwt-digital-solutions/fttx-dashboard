from data import api
import pandas as pd
import plotly.graph_objs as go
from elements import table_styles
import dash_table
from data import collection
import config

colors = config.colors_vwt


def info_table():
    document = collection.get_document(collection="Graphs", client="kpn", graph_name="info_table")
    df = pd.read_json(document['table'], orient='records')
    df = df[document['col']]
    fig = dash_table.DataTable(
        columns=[{"name": i, "id": i} for i in df.columns],
        data=df.to_dict("rows"),
        filter_action="native",
        sort_action="native",
        style_table={'overflowX': 'auto'},
        style_header=table_styles['header'],
        style_cell=table_styles['cell']['action'],
        style_filter=table_styles['filter'],
        css=[{
            'selector': 'table',
            'rule': 'width: 100%;'
        }],
    )
    return fig


# TODO: check if this is still used after removing toggle new_structure_overviews
def pie_chart(client, key="overview"):
    if key == "overview":
        fig = collection.get_document(collection="Data",
                                      client=client,
                                      graph_name="reden_na_overview")['figure']
    else:
        fig = collection.get_document(collection="Data",
                                      client=client,
                                      graph_name="reden_na_projects",
                                      project="pie_na_" + key)['figure']
    data = fig['data']
    trace = go.Pie(data)
    layout = fig['layout']
    data = [trace]
    fig = go.Figure(data, layout=layout)
    return fig


def clickbar_lb(drop_selectie, mask_all):
    fig = collection.get_document(collection="Data",
                                  graph_name='status_bar_chart',
                                  project=drop_selectie,
                                  filter=mask_all
                                  )['bar']
    bar = {}
    for key in fig:
        if 'LB' in key:
            bar[key] = [int(fig[key])]
    labels = ['Schouwen', 'BIS', 'Montage-lasAP', 'Montage-lasDP', 'HAS']
    barLB1HC = dict(x=labels,
                    y=bar['SchouwenLB1'] + bar['BISLB1'] + bar['Montage-lasAPLB1'] + bar['Montage-lasDPLB1'] + bar[
                        'HASLB1'],
                    name='Opgeleverd HC',
                    type='bar',
                    marker=dict(color=colors['green']),
                    )
    barLB1HP = dict(x=labels,
                    y=[0] + [0] + [0] + [0] + bar['HASLB1HP'],
                    name='Opgeleverd zonder HC',
                    type='bar',
                    marker=dict(color=colors['yellow'])
                    )
    barLB0 = dict(x=labels,
                  y=bar['SchouwenLB0'] + bar['BISLB0'] + bar['Montage-lasAPLB0'] + bar['Montage-lasDPLB0'] + bar[
                      'HASLB0'],
                  name='Niet opgeleverd',
                  type='bar',
                  marker=dict(color=colors['red'])
                  )
    fig = dict(data=[barLB1HC, barLB1HP, barLB0],
               layout=dict(barmode='stack',
                           clickmode='event+select',
                           showlegend=True,
                           height=350,
                           plot_bgcolor=colors['plot_bgcolor'],
                           paper_bgcolor=colors['paper_bgcolor'],
                           title={'text': 'Status oplevering per fase (LB)<br>[selectie resets na 3x klikken]:',
                                  'x': 0.5},
                           yaxis={'title': '[aantal woningen]'},
                           ))
    return fig


def clickbar_hb(drop_selectie, mask_all):
    fig = collection.get_document(collection="Data",
                                  graph_name='status_bar_chart',
                                  project=drop_selectie,
                                  filter=mask_all
                                  )['bar']
    bar = {}
    for key in fig:
        if 'HB' in key:
            bar[key] = [int(fig[key])]
    labels = ['Schouwen', 'BIS', 'Montage-lasAP', 'Montage-lasDP', 'HAS']
    barHB1HC = dict(x=labels,
                    y=bar['SchouwenHB1'] + bar['BISHB1'] + bar['Montage-lasAPHB1'] + bar['Montage-lasDPHB1'] + bar[
                        'HASHB1'],
                    name='Opgeleverd HC',
                    type='bar',
                    marker=dict(color=colors['green'])
                    )
    barHB1HP = dict(x=labels,
                    y=[0] + [0] + [0] + [0] + bar['HASHB1HP'],
                    name='Opgeleverd zonder HC',
                    type='bar',
                    marker=dict(color=colors['yellow'])
                    )
    barHB0 = dict(x=labels,
                  y=bar['SchouwenHB0'] + bar['BISHB0'] + bar['Montage-lasAPHB0'] + bar['Montage-lasDPHB0'] + bar[
                      'HASHB0'],
                  name='Niet opgeleverd',
                  type='bar',
                  marker=dict(color=colors['red'])
                  )
    fig = dict(data=[barHB1HC, barHB1HP, barHB0],
               layout=dict(barmode='stack',
                           clickmode='event+select',
                           showlegend=True,
                           height=350,
                           plot_bgcolor=colors['plot_bgcolor'],
                           paper_bgcolor=colors['paper_bgcolor'],
                           title={
                               'text': 'Status oplevering per fase (HB & Duplex)<br>[selectie resets na 3x klikken]:',
                               'x': 0.5},
                           yaxis={'title': '[aantal woningen]'},
                           ))
    return fig


def update_date():
    try:
        date_an_kpn = api.get('/Graphs?id=update_date_kpn')[0]['date'][0:-4].replace('T', ' ')
        date_an_tmobile = api.get('/Graphs?id=update_date_tmobile')[0]['date'][0:-4].replace('T', ' ')
        date_an_dfn = api.get('/Graphs?id=update_date_dfn')[0]['date'][0:-4].replace('T', ' ')
        date_an = min(date_an_kpn, date_an_tmobile, date_an_dfn)
    except IndexError:
        date_an = "[niet beschikbaar]"

    try:
        date_finance_analyse = api.get('/Graphs?id=update_date_kpn_finance')[0]['date'][0:-4].replace('T', ' ')
    except IndexError:
        date_finance_analyse = "[niet beschikbaar]"

    try:
        date_update_baan = api.get('/Graphs?id=update_date_baan_realisation')[0]['date'][0:-4].replace('T', ' ')
    except IndexError:
        date_update_baan = "[niet beschikbaar]"

    try:
        date_con = api.get('/Graphs?id=update_date_fiberconnect')[0]['date'][0:-4].replace('T', ' ')
    except IndexError:
        date_con = "[niet beschikbaar]"
    return [date_an, date_con, date_finance_analyse, date_update_baan]


def ftu_table(data, client):
    if data:
        df = pd.DataFrame()
        df['Project'] = data['FTU0'].keys()
        for key, value in data.items():
            df[key] = value.values()
        df.replace('None', '', inplace=True)
        fig = dash_table.DataTable(
            id=f'table_FTU_{client}',
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict("rows"),
            filter_action="native",
            sort_action="native",
            style_table={'overflowX': 'auto', 'overflowY': 'auto'},
            style_header=table_styles['header'],
            style_cell=table_styles['cell']['action'],
            style_filter=table_styles['filter'],
            css=[{
                'selector': 'table',
                'rule': 'width: 100%;'
            }]
        )
    else:
        fig = get_dummy_table()
    return fig


def get_dummy_table():
    df = pd.DataFrame(columns=['n.b.', 'n.b.', 'n.b.'])
    fig = dash_table.DataTable(
        id='table_FTU',
        columns=[{"name": i, "id": i} for i in df.columns],
        data=df.to_dict("rows"),
        filter_action="native",
        sort_action="native",
        style_table={'overflowX': 'auto', 'overflowY': 'auto'},
        style_header=table_styles['header'],
        style_cell=table_styles['cell']['action'],
        style_filter=table_styles['filter'],
        css=[{
            'selector': 'table',
            'rule': 'width: 100%;'
        }]
    )
    return fig
