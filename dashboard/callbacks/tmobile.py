import dash
from app import app
from dash.dependencies import Input, Output, State

from data import collection
from data.data import completed_status_counts, redenna_by_completed_status, has_planning_by
from layout.components.global_info_list import global_info_list
from layout.components.graphs import pie_chart, completed_status_counts_bar
from layout.pages.tmobile import project_view, new_component
from layout.components import redenna_status_pie
from data.graph import pie_chart as original_pie_chart

from config import colors_vwt as colors

client = "tmobile"


@app.callback(
    [
        Output('project-dropdown-tmobile', 'options')
    ],
    [
        Input('tmobile-overview', 'children')
    ]
)
def load_dropdown(dummy_data):
    return [collection.get_document(collection="Data",
                                    client=client,
                                    graph_name="project_names")['filters']]


@app.callback(
    Output('info-container-tmobile', 'children'),
    [
        Input('tmobile-overview', 'children')
    ]
)
def load_project_info(dummy_data):
    jaaroverzicht = collection.get_document(collection="Data", graph_name="jaaroverzicht", client=client)
    jaaroverzicht_list = [
        dict(id_="info_globaal_container0",
             title='Outlook',
             text="HPend afgesproken: ",
             value='10000'),
        dict(id_="info_globaal_container1", title='Realisatie (FC)', text="HPend gerealiseerd: ",
             value=jaaroverzicht['real']),
        dict(id_="info_globaal_container2", title='Planning (VWT)', text="HPend gepland vanaf nu: ",
             value=jaaroverzicht['plan']),
        dict(id_="info_globaal_container3", title='Voorspelling (VQD)',
             text="HPend voorspeld vanaf nu: ", value='1000'),
        dict(id_="info_globaal_container5", title='Werkvoorraad HAS',
             value=str(collection.get_document(
                 collection="Data", client=client, graph_name="voorraadvormend")['all'])),
        dict(id_="info_globaal_container4", title='Actuele HC / HPend',
             value='n.v.t.'),
        dict(id_="info_globaal_container4", title='Ratio <8 weken',
             value='0.66'),
    ]
    return [
        global_info_list(items=jaaroverzicht_list,
                         className="container-display")
    ]


@app.callback(
    Output('month-overview-tmobile', 'figure'),
    [
        Input('tmobile-overview', 'children')
    ]
)
def load_month_overview(dummy_data):
    return new_component.get_html_overview(has_planning_by('month', client))


@app.callback(
    Output('week-overview-tmobile', 'figure'),
    [
        Input('tmobile-overview', 'children')
    ]
)
def load_week_overview(dummy_data):
    return new_component.get_html_overview(has_planning_by('week', client))


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
        return [{'display': 'block'}, project_view.get_html("tmobile")]
    return [{'display': 'none'}, project_view.get_html("tmobile")]


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
        Output('status-count-filter-tmobile', 'data')
    ],
    [
        Input('status-counts-laagbouw-tmobile', 'clickData'),
        Input('status-counts-hoogbouw-tmobile', 'clickData'),
        Input('overview-reset', 'n_clicks')
    ],
    [
        State('status-count-filter-tmobile', "data")
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
        Output('status-counts-laagbouw-tmobile', 'figure'),
        Output('status-counts-hoogbouw-tmobile', 'figure')
    ],
    [
        Input('status-count-filter-tmobile', 'data'),
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
        Output('redenna_project_tmobile', 'figure')
    ],
    [
        Input('status-count-filter-tmobile', 'data'),
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
    Output('pie_chart_overview_tmobile', 'figure'),
    [Input('week-overview-tmobile', 'clickData'),
     Input('month-overview-tmobile', 'clickData'),
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
                return original_pie_chart('tmobile')
            for point in trigger['value']['points']:
                first_day_of_period = point['customdata']
                break
            break

        redenna_by_period = collection.get_document(collection="Data",
                                                    client="tmobile",
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
    return original_pie_chart('tmobile')
