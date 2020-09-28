import dash
from app import app
from dash.dependencies import Input, Output, State

from data import collection
from data.data import completed_status_counts
from layout.components.graphs import pie_chart, completed_status_counts_bar
from layout.pages.tmobile import project_view
from data.graph import pie_chart as original_pie_chart

from config import colors_vwt as colors

client = "t-mobile"


@app.callback(
    [
        Output(component_id="tmobile-overview", component_property='style')
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
        Output(component_id="tmobile-project-view", component_property='style'),
        Output("tmobile-project-view", "children")
    ],
    [
        Input('project-dropdown-tmobile', 'value')
    ]
)
def tmobile_project_view(dropdown_selection):
    if dropdown_selection:
        return [{'display': 'block'}, project_view.get_html(dropdown_selection, "t-mobile")]
    return [{'display': 'none'}, project_view.get_html(dropdown_selection, "t-mobile")]


@app.callback(
    [
        Output(component_id="project-dropdown-tmobile", component_property='value')
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
        status_counts = completed_status_counts(project_name, click_filter=click_filter)
        laagbouw = completed_status_counts_bar.get_fig(status_counts.laagbouw,
                                                       title="Status oplevering per fase (LB)")
        hoogbouw = completed_status_counts_bar.get_fig(status_counts.hoogbouw,
                                                       title="Status oplevering per fase (HB & Duplex)")
        return laagbouw, hoogbouw
    return {'data': None, 'layout': None}, {'data': None, 'layout': None}


@app.callback(
    Output('pie_chart_overview_t-mobile', 'figure'),
    [Input('week-overview', 'clickData'),
     Input('month-overview', 'clickData'),
     Input('overview-reset', 'n_clicks')
     ]
)
def display_click_data(week_click_data, month_click_data, reset):
    ctx = dash.callback_context
    first_day_of_period = ""
    period = ""
    if ctx.triggered:
        for trigger in ctx.triggered:
            period, _, _ = trigger['prop_id'].partition("-")
            if period == "overview":
                return original_pie_chart('t-mobile')
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
