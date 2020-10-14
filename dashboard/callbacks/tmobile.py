import dash

from app import app
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from data import collection
from layout.components.graphs import pie_chart
import dash_bootstrap_components as dbc
from layout.components.figure import figure
from layout.components.indicator import indicator
from config import colors_vwt as colors

client = "tmobile"


@app.callback(
    [
        Output("modal-sm", "is_open"),
        Output(f"indicator-modal-{client}", 'figure')
    ],
    [
        Input(f"indicator-late-{client}", "n_clicks"),
        Input(f"indicator-limited_time-{client}", "n_clicks"),
        Input(f"indicator-on_time-{client}", "n_clicks"),
        Input("close-sm", "n_clicks"),
    ],
    [
        State("modal-sm", "is_open"),
        State(f"indicator-data-{client}", "data")
    ]
)
def indicator_modal(late_clicks, limited_time_clicks, on_time_clicks, close_clicks, is_open, result):
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    print(changed_id)
    if "indicator" in changed_id and (late_clicks or limited_time_clicks or on_time_clicks):
        key = changed_id.partition("-")[-1].partition("-")[0]
        print(key)
        figure = pie_chart.get_html(labels=list(result[key]['cluster_redenna'].keys()),
                                    values=list(result[key]['cluster_redenna'].values()),
                                    title="Reden na",
                                    colors=[
                                        colors['green'],
                                        colors['yellow'],
                                        colors['red'],
                                        colors['vwt_blue'],
                                    ])

        return [not is_open, figure]

    if close_clicks:
        return [not is_open, {'data': None, 'layout': None}]
    return [is_open, {'data': None, 'layout': None}]


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

    indicator_types = ['late', 'limited_time', 'on_time']
    indicators = collection.get_document(collection="Data",
                                         graph_name="project_indicators",
                                         project=dropdown_selection,
                                         client=client)
    indicator_info = [indicator(value=indicators[el]['counts'],
                                previous_value=indicators[el]['counts_prev'],
                                title=indicators[el]['title'],
                                sub_title=indicators[el]['subtitle'],
                                font_color=indicators[el]['font_color'],
                                id=f"indicator-{el}-{client}") for el in indicator_types]
    indicator_info = indicator_info + [
        dbc.Modal(
            [
                dbc.ModalBody(
                    figure(graph_id=f"indicator-modal-{client}",
                           className="",
                           figure={'data': None, 'layout': None})
                ),
                dbc.ModalFooter(
                    dbc.Button("Close", id="close-sm", className="ml-auto")
                ),
            ],
            id="modal-sm",
            size="lg",
            centered=True,
        )
    ]

    return [indicator_info, indicators]
