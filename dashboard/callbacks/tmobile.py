import dash

from app import app
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from data import collection
from data.data import completed_status_counts, redenna_by_completed_status
from layout.components.graphs import pie_chart, completed_status_counts_bar
import dash_bootstrap_components as dbc
from layout.components.figure import figure
from layout.components.indicator import indicator
from layout.components import redenna_status_pie
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

    indicator_types = ['late', 'limited_time', 'on_time', 'ready_for_has']
    indicators = collection.get_document(collection="Data",
                                         graph_name="project_indicators",
                                         project=dropdown_selection,
                                         client=client)
    indicator_info = [indicator(value=indicators[el]['counts'],
                                previous_value=indicators[el]['counts_prev'],
                                title=indicators[el]['title'],
                                sub_title=indicators[el].get('subtitle', " "),
                                font_color=indicators[el].get('font_color', 'black'),
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


@app.callback(
    [
        Output(f'status-count-filter-{client}', 'data')
    ],
    [
        Input(f'status-counts-laagbouw-{client}', 'clickData'),
        Input(f'status-counts-hoogbouw-{client}', 'clickData'),
        Input(f'overview-reset-{client}', 'n_clicks')
    ],
    [
        State(f'status-count-filter-{client}', "data")
    ]
)
def set_status_click_filter(laagbouw_click, hoogbouw_click, reset_button, click_filter):
    ctx = dash.callback_context
    if not click_filter:
        click_filter = {}
    if isinstance(click_filter, list):
        click_filter = click_filter[0]
    if ctx.triggered:
        for trigger in ctx.triggered:
            if trigger['prop_id'] == list(ctx.inputs.keys())[2]:
                return [None]

            for point in trigger['value']['points']:
                category, _, cat_filter = point['customdata'].partition(";")
                click_filter[category] = cat_filter
                return [click_filter]


@app.callback(
    [
        Output(f'status-counts-laagbouw-{client}', 'figure'),
        Output(f'status-counts-hoogbouw-{client}', 'figure')
    ],
    [
        Input(f'status-count-filter-{client}', 'data'),
        Input(f'project-dropdown-{client}', 'value')
    ]
)
def update_graphs_using_status_clicks(click_filter, project_name):
    if project_name:
        status_counts = completed_status_counts(project_name, click_filter=click_filter, client=client)
        laagbouw = completed_status_counts_bar.get_fig(status_counts.laagbouw,
                                                       title="Status oplevering per fase (LB)")
        hoogbouw = completed_status_counts_bar.get_fig(status_counts.hoogbouw,
                                                       title="Status oplevering per fase (HB & Duplex)")
        return laagbouw, hoogbouw
    return {'data': None, 'layout': None}, {'data': None, 'layout': None}


@app.callback(
    [
        Output(f'redenna_project_{client}', 'figure')
    ],
    [
        Input(f'status-count-filter-{client}', 'data'),
        Input(f'project-dropdown-{client}', 'value')
    ]
)
def update_redenna_status_clicks(click_filter, project_name):
    if project_name:
        redenna_counts = redenna_by_completed_status(project_name, click_filter=click_filter, client=client)
        redenna_pie = redenna_status_pie.get_fig(redenna_counts,
                                                 title="Opgegeven reden na",
                                                 colors=[
                                                     colors['vwt_blue'],
                                                     colors['yellow'],
                                                     colors['red'],
                                                     colors['green']
                                                 ])
        return [redenna_pie]
    return [{'data': None, 'layout': None}]
