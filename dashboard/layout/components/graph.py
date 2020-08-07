from analyse.functions import from_rd
from data import api
import pandas as pd
import plotly.graph_objs as go
from elements import table_styles
import json
import dash_table


def graph_new(url_id, target=None):
    fig = api.get(f'/Graphs?id={url_id}')[0]['target']
    print(fig)
    return fig


def jaaroverzicht_graph(value):
    return api.get('/Graphs?id=jaaroverzicht')[0][value]


def graph(flag, drop_selectie, mask_all):
    if flag == 80:
        fig = api.get('/Graphs?id=jaaroverzicht')[0]['target']

    if flag == 81:
        fig = api.get('/Graphs?id=jaaroverzicht')[0]['real']

    if flag == 82:
        fig = api.get('/Graphs?id=jaaroverzicht')[0]['plan']

    if flag == 83:
        fig = api.get('/Graphs?id=jaaroverzicht')[0]

    if flag == 84:
        fig = api.get('/Graphs?id=jaaroverzicht')[0]['HC_HPend']

    if flag == 85:
        date_an = api.get('/Graphs?id=update_date')[0]['date']
        date_con = api.get('/Graphs?id=update_date_consume')[0]['date']
        fig = min([date_an, date_con])

    if flag == 86:
        fig = api.get('/Graphs?id=jaaroverzicht')[0]['HAS_werkvoorraad']

    # prognose
    if flag == 1:
        fig = api.get('/Graphs?id=' + 'project_' + drop_selectie)[0]['figure']
        for i, item in enumerate(fig['data']):
            fig['data'][i]['x'] = pd.to_datetime(item['x'])

    # project speed
    if flag == 2:
        fig = api.get('/Graphs?id=project_performance')[0]['figure']

    # labels
    if flag == 3:
        fig = api.get('/Graphs?id=pnames')[0]['filters']

    # targets
    if flag == 41:
        fig = api.get('/Graphs?id=graph_targets_W')[0]['figure']

    if flag == 42:
        fig = api.get('/Graphs?id=graph_targets_M')[0]['figure']

    # clickbar LB
    if flag == 5:
        fig = api.get('/Graphs?id=' + drop_selectie + '_bar_filters_' + mask_all)[0]['bar']
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
                        marker=dict(color='rgb(0, 200, 0)'),
                        )
        barLB1HP = dict(x=labels,
                        y=[0] + [0] + [0] + [0] + bar['HASLB1HP'],
                        name='Opgeleverd zonder HC',
                        type='bar',
                        marker=dict(color='rgb(200, 200, 0)')
                        )
        barLB0 = dict(x=labels,
                      y=bar['SchouwenLB0'] + bar['BISLB0'] + bar['Montage-lasAPLB0'] + bar['Montage-lasDPLB0'] + bar[
                          'HASLB0'],
                      name='Niet opgeleverd',
                      type='bar',
                      marker=dict(color='rgb(200, 0, 0)')
                      )
        fig = dict(data=[barLB1HC, barLB1HP, barLB0],
                   layout=dict(barmode='stack',
                               clickmode='event+select',
                               showlegend=True,
                               height=350,
                               title={'text': 'Status oplevering per fase (LB)<br>[selectie resets na 3x klikken]:',
                                      'x': 0.5},
                               yaxis={'title': '[aantal woningen]'},
                               ))

    # clickbar HB
    if flag == 6:
        fig = api.get('/Graphs?id=' + drop_selectie + '_bar_filters_' + mask_all)[0]['bar']
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
                        marker=dict(color='rgb(0, 200, 0)')
                        )
        barHB1HP = dict(x=labels,
                        y=[0] + [0] + [0] + [0] + bar['HASHB1HP'],
                        name='Opgeleverd zonder HC',
                        type='bar',
                        marker=dict(color='rgb(200, 200, 0)')
                        )
        barHB0 = dict(x=labels,
                      y=bar['SchouwenHB0'] + bar['BISHB0'] + bar['Montage-lasAPHB0'] + bar['Montage-lasDPHB0'] + bar[
                          'HASHB0'],
                      name='Niet opgeleverd',
                      type='bar',
                      marker=dict(color='rgb(200, 0, 0)')
                      )
        fig = dict(data=[barHB1HC, barHB1HP, barHB0],
                   layout=dict(barmode='stack',
                               clickmode='event+select',
                               showlegend=True,
                               height=350,
                               title={
                                   'text': 'Status oplevering per fase (HB & Duplex)<br>[selectie resets na 3x klikken]:',
                                   'x': 0.5},
                               yaxis={'title': '[aantal woningen]'},
                               ))

    # geomap & data table
    if flag == 7:
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
                        colorscale=['green', 'yellow', 'red'],
                        reversescale=True,
                        size=normalized_size * 7,
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
                title="Status oplevering per woning (kleine marker) & DP (grote marker)<br>[groen = opgeleverd, rood = niet opgeleverd]",
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

        df['uitleg redenna'] = df['redenna'].map(api.get('/Graphs?id=reden_mapping')[0]['map'])
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

    if flag == 8:
        df = pd.read_json(api.get('/Graphs?id=info_table')[0]['table'], orient='records')
        df = df[api.get('/Graphs?id=info_table')[0]['col']]
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

    if flag == 9:
        df = pd.DataFrame(columns=['Project', 'FTU0', 'FTU1'])
        df['Project'] = list(api.get('/Graphs?id=analysis')[0]['FTU0'].keys())
        df['FTU0'] = list(api.get('/Graphs?id=analysis')[0]['FTU0'].values())
        df['FTU1'] = list(api.get('/Graphs?id=analysis')[0]['FTU1'].values())
        fig = dash_table.DataTable(
            id='table_FTU',
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
            editable=False,
        )

    if flag == 10:
        fig = get_pie(drop_selectie)
    if flag == 11:
        fig = get_pie('overview')
    return fig


def get_pie(key):
    fig = api.get('/Graphs?id=pie_na_' + key)
    data = fig[0]['figure']['data']
    trace = go.Pie(data)
    layout = fig[0]['figure']['layout']
    data = [trace]
    fig = dict(data=data,
               layout=layout)
    return fig
