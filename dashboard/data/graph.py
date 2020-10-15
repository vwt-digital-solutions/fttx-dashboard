from analyse_dashboard.analyse.functions import from_rd
from data import api, local
import pandas as pd
import plotly.graph_objs as go
from elements import table_styles
import json
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
    date_an = api.get('/Graphs?id=update_date')[0]['date']
    date_con = api.get('/Graphs?id=update_date_consume')[0]['date']
    return [date_an, date_con]


def ftu_table(data, client):
    if data:
        df = pd.DataFrame(columns=['Project', 'Eerste HAS aansluiting (FTU0)', 'Laatste HAS aansluiting (FTU1)'])
        df['Project'] = data['FTU0'].keys()
        df['Eerste HAS aansluiting (FTU0)'] = data['FTU0'].values()
        df['Laatste HAS aansluiting (FTU1)'] = data['FTU1'].values()
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
            }],
            editable=False,
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
        }],
        editable=False,
    )
    return fig


def geomap_data_table(drop_selectie, mask_all):
    if mask_all == '0':
        records = api.get('/Projects?project=' + drop_selectie)
        df = pd.DataFrame(records)
    else:
        mask = json.loads(api.get('/Graphs?id=' + drop_selectie + '_bar_filters_' + mask_all)[0]['mask'])
        dataframe = []
        for m in mask:
            dataframe += api.get('/Projects?sleutel=' + m)
        df = pd.DataFrame(dataframe)

    if not df[~df['x_locatie_rol'].isna()].empty:

        df['clr'] = 50
        df.loc[df['opleverdatum'].isna(), ('clr')] = 0
        df['clr-DP'] = 0
        df.loc[df['opleverstatus'] != 0, ('clr-DP')] = 50  # 25 == geel
        df['x_locatie_rol'] = df['x_locatie_rol'].str.replace(',', '.').astype(float)
        df['y_locatie_rol'] = df['y_locatie_rol'].str.replace(',', '.').astype(float)
        df['x_locatie_dp'] = df['x_locatie_dp'].str.replace(',', '.').astype(float)
        df['y_locatie_dp'] = df['y_locatie_dp'].str.replace(',', '.').astype(float)
        df['Lat'], df['Long'] = from_rd(df['x_locatie_rol'], df['y_locatie_rol'])
        df['Lat_DP'], df['Long_DP'] = from_rd(df['x_locatie_dp'], df['y_locatie_dp'])
        df['Size'] = 7
        df['Size_DP'] = 14

        # this is a default public token obtained from a free account on https://account.mapbox.com/
        # and can there be refreshed at any moment
        mapbox_at = api.get('/Graphs?id=token_mapbox')[0]['token']
        normalized_size = df['Size_DP'].to_list() + df['Size'].to_list()
        map_data = [
            go.Scattermapbox(
                lat=df['Lat_DP'].to_list() + df['Lat'].to_list(),
                lon=df['Long_DP'].to_list() + df['Long'].to_list(),
                mode='markers',
                marker=dict(
                    cmax=50,
                    cmin=0,
                    color=df['clr-DP'].to_list() + df['clr'].to_list(),
                    colorscale=[colors['green'], colors['yellow'], colors['red']],
                    reversescale=True,
                    size=normalized_size * 7,
                    plot_bgcolor=colors['plot_bgcolor'],
                    paper_bgcolor=colors['paper_bgcolor']
                ),
                text=df['clr'],
                hoverinfo='text'
            )
        ]
        map_layout = dict(
            autosize=True,
            automargin=True,
            margin={'l': 30, 'r': 30, 'b': 30, 't': 120},
            height=500,
            hovermode="closest",
            plot_bgcolor="#F9F9F9",
            paper_bgcolor="#F9F9F9",
            legend=dict(font=dict(size=10), orientation="h"),
            title="Status oplevering per woning (kleine marker) & DP"
                  "(grote marker)<br>[groen = opgeleverd, rood = niet opgeleverd]",
            mapbox=dict(
                accesstoken=mapbox_at,
                style="light",
                center=dict(lon=df['Long'].mean(), lat=df['Lat'].mean()),
                zoom=13,
            ),
        )

        fig = dict(geo={'data': map_data, 'layout': map_layout})
    else:
        fig = dict(geo={'data': None, 'layout': dict()})

    df['uitleg redenna'] = df['redenna'].map(local.reden_mapping)
    df = df[['sleutel', 'opleverdatum', 'hasdatum', 'opleverstatus', 'uitleg redenna']].sort_values(by='hasdatum')
    df_table = dash_table.DataTable(
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
    fig['table'] = df_table
    return fig
