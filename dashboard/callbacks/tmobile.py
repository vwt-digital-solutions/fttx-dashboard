import dash
from app import app
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from data import collection
from data.data import completed_status_counts, redenna_by_completed_status
from layout.components.figure import figure
from layout.components.graphs import pie_chart, completed_status_counts_bar
from layout.components.indicator import indicator
from layout.components import redenna_status_pie
from data.graph import pie_chart as original_pie_chart

from config import colors_vwt as colors

client = "t-mobile"


@app.callback(
    [
        Output("tmobile-overview", 'style')
    ],
    [
        Input('project-dropdown-tmobile', 'value')
    ]
)
def tmobile_overview(dropdown_selection):
    if dropdown_selection:
        return [{'display': 'none'}]
    return [{'display': 'block'}]


@app.callback(
    [
        Output("tmobile-project-view", 'style'),
    ],
    [
        Input('project-dropdown-tmobile', 'value')
    ]
)
def tmobile_project_view(dropdown_selection):
    if dropdown_selection:
        return [{'display': 'block'}]
    return [{'display': 'none'}]


@app.callback(
    [
        Output("quality-measures-t-mobile", "children"),
        Output(f"indicator-data-{client}", 'data')
    ],
    [
        Input('project-dropdown-tmobile', 'value')
    ]
)
def tmobile_quality_measures(dropdown_selection):
    if dropdown_selection:
        result = collection.get_document(collection="Data",
                                         client=client,
                                         graph_name="quality_measure",
                                         project=dropdown_selection)

        indicators = [
            indicator(value=result['late']['counts'],
                      previous_value=result['late']['counts_prev'],
                      title="Order te laat",
                      sub_title="> 12 weken",
                      font_color="red",
                      id=f"indicator-late-{client}"),
            indicator(value=result['limited_time']['counts'],
                      previous_value=result['limited_time']['counts_prev'],
                      title="Order nog beperkte tijd",
                      sub_title="> 8 weken < 12 weken",
                      font_color="orange",
                      id=f"indicator-limited_time-{client}"),
            indicator(value=result['on_time']['counts'],
                      previous_value=result['on_time']['counts_prev'],
                      title="Order op tijd",
                      sub_title="< 8 weken",
                      font_color="green",
                      id=f"indicator-on_time-{client}"),
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
        return [indicators, result]
    return [None, None]


@app.callback(
    [
        Output("modal-sm", "is_open"),
        Output(f"indicator-modal-{client}", 'figure')
    ],
    [
        Input("indicator-late-t-mobile", "n_clicks"),
        Input("indicator-limited_time-t-mobile", "n_clicks"),
        Input("indicator-on_time-t-mobile", "n_clicks"),
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
        Output("project-dropdown-tmobile", 'value')
    ],
    [
        Input('overzicht-button-tmobile', 'n_clicks')
    ]
)
def tmobile_overview_button(_):
    return [None]


@app.callback(
    [
        Output('status-count-filter-t-mobile', 'data')
    ],
    [
        Input('status-counts-laagbouw-t-mobile', 'clickData'),
        Input('status-counts-hoogbouw-t-mobile', 'clickData'),
        Input('overview-reset', 'n_clicks')
    ],
    [
        State('status-count-filter-t-mobile', "data")
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
            if trigger['prop_id'] == "overview-reset.n_clicks":
                return [None]

            for point in trigger['value']['points']:
                category, _, cat_filter = point['customdata'].partition(";")
                click_filter[category] = cat_filter
                return [click_filter]


@app.callback(
    [
        Output('status-counts-laagbouw-t-mobile', 'figure'),
        Output('status-counts-hoogbouw-t-mobile', 'figure')
    ],
    [
        Input('status-count-filter-t-mobile', 'data'),
        Input('project-dropdown-tmobile', 'value')
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
        Output('redenna_project_t-mobile', 'figure')
    ],
    [
        Input('status-count-filter-t-mobile', 'data'),
        Input('project-dropdown-tmobile', 'value')
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


@app.callback(
    Output('pie_chart_overview_t-mobile', 'figure'),
    [Input('week-overview', 'clickData'),
     Input('month-overview', 'clickData'),
     Input('overview-reset', 'n_clicks')
     ],
    [
        State('pie_chart_overview_t-mobile', 'figure')
    ]
)
def display_click_data(week_click_data, month_click_data, reset, original_figure):
    ctx = dash.callback_context
    first_day_of_period = ""
    period = ""
    if ctx.triggered:
        for trigger in ctx.triggered:
            period, _, _ = trigger['prop_id'].partition("-")
            if period == "overview":
                return original_pie_chart('t-mobile')
            if trigger['value']['points'][0]['curveNumber'] != 1:
                return original_figure
            for point in trigger['value']['points']:
                first_day_of_period = point['customdata']
                break
            break

        redenna_by_period = collection.get_document(collection="Data",
                                                    client="t-mobile",
                                                    graph_name=f"redenna_by_{period}")

        fig = pie_chart.get_html(labels=list(redenna_by_period.get(first_day_of_period, dict()).keys()),
                                 values=list(redenna_by_period.get(first_day_of_period, dict()).values()),
                                 title=f"Reden na voor de {period} {first_day_of_period}",
                                 colors=[
                                     colors['green'],
                                     colors['yellow'],
                                     colors['red'],
                                     colors['vwt_blue'],
                                 ])

        return fig
    return original_pie_chart('t-mobile')
