import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from google.cloud import firestore

from app import app
from config import colors_vwt as colors
from data import collection
from layout.components.figure import figure
from layout.components.graphs import pie_chart
from layout.components.indicator import indicator

import pandas as pd

client = "tmobile"


@app.callback(
    [
        Output("modal-sm", "is_open"),
        Output(f"indicator-modal-{client}", 'figure'),
        Output(f"indicator-download-{client}", 'href')
    ],
    [
        Input(f"indicator-late-{client}", "n_clicks"),
        Input(f"indicator-limited_time-{client}", "n_clicks"),
        Input(f"indicator-on_time-{client}", "n_clicks"),
        Input("close-sm", "n_clicks"),
    ],
    [
        State("modal-sm", "is_open"),
        State(f"indicator-data-{client}", "data"),
        State(f'project-dropdown-{client}', 'value')
    ]
)
def indicator_modal(late_clicks, limited_time_clicks, on_time_clicks, close_clicks, is_open, result, project):
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    if "indicator" in changed_id and (late_clicks or limited_time_clicks or on_time_clicks):
        key = changed_id.partition("-")[-1].partition("-")[0]
        # Sorted the cluster redenna dict here, so that the pie chart pieces have the proper color:
        cluster_redenna_sorted_dict = dict(sorted(result[key]['cluster_redenna'].items()))
        figure = pie_chart.get_html(labels=list(cluster_redenna_sorted_dict.keys()),
                                    values=list(cluster_redenna_sorted_dict.values()),
                                    title="Reden na",
                                    colors=[
                                        colors['green'],
                                        colors['yellow'],
                                        colors['red'],
                                        colors['vwt_blue'],
                                    ])

        return [not is_open, figure, f'/dash/order_wait_download?wait_category={key}&project={project}']

    if close_clicks:
        return [not is_open, {'data': None, 'layout': None}, ""]
    return [is_open, {'data': None, 'layout': None}, ""]


@app.callback(
    [
        Output(f"indicators-{client}", "children"),
        Output(f"indicator-data-{client}", 'data')
    ],
    [
        Input(f'project-dropdown-{client}', 'value')
    ]
)
def update_indicators(dropdown_selection):
    if dropdown_selection is None:
        raise PreventUpdate

    indicator_types = ['on_time', 'limited_time', 'late', 'ratio', 'ready_for_has']
    indicators = collection.get_document(collection="Data",
                                         graph_name="project_indicators",
                                         project=dropdown_selection,
                                         client=client)
    indicator_info = [indicator(value=indicators[element]['counts'],
                                previous_value=indicators[element].get('counts_prev', None),
                                title=indicators[element]['title'],
                                sub_title=indicators[element].get('subtitle', " "),
                                font_color=indicators[element].get('font_color', 'black'),
                                invert_delta=indicators[element].get("invert_delta", False),
                                percentage=indicators[element].get("percentage"),
                                id=f"indicator-{element}-{client}") for element in indicator_types]
    indicator_info = indicator_info + [
        dbc.Modal(
            [
                dbc.ModalBody(
                    figure(graph_id=f"indicator-modal-{client}",
                           className="",
                           figure={'data': None, 'layout': None})
                ),
                dbc.ModalFooter(
                    children=[
                        html.A(
                            dbc.Button("Download", id="download-indicator", className="ml-auto"),
                            id=f"indicator-download-{client}",
                            href="/dash/urlToDownload"),
                        dbc.Button("Close", id="close-sm", className="ml-auto"),
                    ]
                ),
            ],
            id="modal-sm",
            size="lg",
            centered=True,
        )
    ]

    return [indicator_info, indicators]


# update FTU table for editing
@app.callback(
    [
        Output(f'table_FTU_{client}', 'editable'),
    ],
    [
        Input('ww', 'value'),
    ],
)
def FTU_table_editable(ww):
    return [ww == 'FttX']


# update firestore given edit FTU table
@app.callback(
    [
        Output(f'project-performance-{client}', 'figure'),
    ],
    [
        Input(f'table_FTU_{client}', 'data'),
    ],
)
def FTU_update(data):
    print('start updating FTU tabel')
    record = dict(graph_name='project_dates', client=client)
    df = pd.DataFrame(data)
    updated_dict = {}
    for col in df:
        if col != "Project":
            updated_dict[col] = dict(zip(df['Project'], df[col]))
    record['record'] = updated_dict
    print(record)
    firestore.Client().collection('Data').document(f'{client}_project_dates').set(record)
    output = collection.get_graph(client=client, graph_name='project_performance')
    return [output]
