import dash
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from data.graph import pie_chart as original_pie_chart
from layout.components.graphs import pie_chart, completed_status_counts_bar
from app import app

import config
from data import collection
from data.data import has_planning_by, completed_status_counts, redenna_by_completed_status
from layout.components.global_info_list import global_info_list
from layout.components.graphs import overview_bar_chart
from config import colors_vwt as colors
from layout.components import redenna_status_pie

for client in config.client_config.keys():
    @app.callback(
        [
            Output(f"{client}-overview", 'style')
        ],
        [
            Input(f'project-dropdown-{client}', 'value')
        ]
    )
    def overview(dropdown_selection):
        if dropdown_selection:
            return [{'display': 'none'}]
        return [{'display': 'block'}]

    @app.callback(
        [
            Output(f"{client}-project-view", 'style'),
        ],
        [
            Input(f'project-dropdown-{client}', 'value')
        ]
    )
    def project_view(dropdown_selection):
        if dropdown_selection:
            return [{'display': 'block'}]
        return [{'display': 'none'}]

    @app.callback(
        [
            Output(f"project-dropdown-{client}", 'value'),
        ],
        [
            Input(f"project-performance-{client}", 'clickData'),
            Input(f'overzicht-button-{client}', 'n_clicks')
        ]
    )
    def update_dropdown(project_performance_click, overzicht_click):
        ctx = dash.callback_context
        for trigger in ctx.triggered:
            if trigger['prop_id'] == list(ctx.inputs.keys())[0]:
                return [project_performance_click['points'][0]['text']]
            elif trigger['prop_id'] == list(ctx.inputs.keys())[1]:
                return [None]
        return [None]

    @app.callback(
        [
            Output(f'project-dropdown-{client}', 'options')
        ],
        [
            Input(f'{client}-overview', 'children')
        ]
    )
    def load_dropdown(dummy_data, client=client):
        return [collection.get_document(collection="Data",
                                        client=client,
                                        graph_name="project_names")['filters']]

    @app.callback(
        Output(f'info-container-{client}', 'children'),
        [
            Input(f'{client}-overview', 'children')
        ]
    )
    def load_project_info(dummy_data, client=client):
        jaaroverzicht = collection.get_document(collection="Data", graph_name="jaaroverzicht", client=client)
        # temp fix for planning DFN since we use dummy data
        if client != 'kpn':
            jaaroverzicht['plan'] = 'n.v.t.'
        jaaroverzicht_list = [
            dict(id_="info_globaal_container0",
                 title='Target',
                 text="HPend afgesproken: ",
                 value=jaaroverzicht.get('target', 'n.v.t.')),
            dict(id_="info_globaal_container1", title='Realisatie (HPend)', text="HPend gerealiseerd: ",
                 value=jaaroverzicht.get('real', 'n.v.t.')),
            dict(id_="info_globaal_container1", title='Realisatie (BIS)', text="BIS gerealiseerd: ",
                 value=jaaroverzicht.get('bis_gereed', 'n.v.t.')),
            dict(id_="info_globaal_container2", title='Planning (VWT)', text="HPend gepland vanaf nu: ",
                 value=jaaroverzicht.get('plan', 'n.v.t.')),
            dict(id_="info_globaal_container3", title='Voorspelling (VQD)',
                 text="HPend voorspeld vanaf nu: ", value=jaaroverzicht.get('prog', 'n.v.t'),
                 className=jaaroverzicht.get("prog_c", 'n.v.t.') + "  column"),
            dict(id_="info_globaal_container5", title='Werkvoorraad HAS',
                 value=jaaroverzicht.get('HAS_werkvoorraad', 'n.v.t.')),
            dict(id_="info_globaal_container4", title='Actuele HC / HPend',
                 value=jaaroverzicht.get('HC_HPend', 'n.v.t.')),
            dict(id_="info_globaal_container4", title='Ratio <8 weken',
                 value=jaaroverzicht.get('ratio_op_tijd', 'n.v.t.')),
        ]
        return [
            global_info_list(items=jaaroverzicht_list,
                             className="container-display")
        ]

    @app.callback(
        Output(f'month-overview-{client}', 'figure'),
        [
            Input(f'{client}-overview', 'children')
        ]
    )
    def load_month_overview(dummy_data, client=client):
        return overview_bar_chart.get_fig(has_planning_by('month', client))

    @app.callback(
        Output(f'week-overview-{client}', 'figure'),
        [
            Input(f'{client}-overview', 'children')
        ]
    )
    def load_week_overview(dummy_data, client=client):
        print("Running week overview")
        return overview_bar_chart.get_fig(has_planning_by('week', client))

    # TODO Dirty fix with hardcoded client name here, to prevent graph not loading for KPN, for which this function
    # does not work correctly yet.
    @app.callback(
        Output(f'pie_chart_overview_{client}', 'figure'),
        [Input(f'week-overview-{client}', 'clickData'),
         Input(f'month-overview-{client}', 'clickData'),
         Input(f'overview-reset-{client}', 'n_clicks')
         ]
    )
    def display_click_data(week_click_data, month_click_data, reset, client=client):
        if client == 'kpn':
            return original_pie_chart(client)
        ctx = dash.callback_context
        first_day_of_period = ""
        period = ""
        if ctx.triggered:
            for trigger in ctx.triggered:
                period, _, _ = trigger['prop_id'].partition("-")
                if period == "overview":
                    return original_pie_chart(client)
                if trigger['value']['points'][0]['curveNumber'] != 1:
                    raise PreventUpdate
                for point in trigger['value']['points']:
                    first_day_of_period = point['customdata']
                    break
                break

            redenna_by_period = collection.get_document(collection="Data",
                                                        client=client,
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
        return original_pie_chart(client)

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
            Output(f'status-counts-hoogbouw-{client}', 'figure'),
            Output(f'status-counts-laagbouw-{client}-container', 'style'),
            Output(f'status-counts-hoogbouw-{client}-container', 'style'),
        ],
        [
            Input(f'status-count-filter-{client}', 'data'),
            Input(f'project-dropdown-{client}', 'value')
        ]
    )
    def update_graphs_using_status_clicks(click_filter, project_name, client=client):
        if project_name:
            status_counts = completed_status_counts(project_name, click_filter=click_filter, client=client)
            laagbouw = completed_status_counts_bar.get_fig(status_counts.laagbouw,
                                                           title="Status oplevering per fase (LB)")
            laagbouw_style = {'display': 'block'} if laagbouw else {'display': 'none'}
            hoogbouw = completed_status_counts_bar.get_fig(status_counts.hoogbouw,
                                                           title="Status oplevering per fase (HB & Duplex)")
            hoogbouw_style = {'display': 'block'} if hoogbouw else {'display': 'none'}
            return laagbouw, hoogbouw, laagbouw_style, hoogbouw_style
        return {'data': None, 'layout': None}, {'data': None, 'layout': None}, {'display': 'block'}, {'display': 'block'}

    @app.callback(
        [
            Output(f'redenna_project_{client}', 'figure')
        ],
        [
            Input(f'status-count-filter-{client}', 'data'),
            Input(f'project-dropdown-{client}', 'value')
        ]
    )
    def update_redenna_status_clicks(click_filter, project_name, client=client):
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
