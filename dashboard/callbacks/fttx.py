import dash
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

from data.graph import pie_chart as original_pie_chart
from layout.components.graphs import pie_chart
from app import app

import config
from data import collection
from data.data import has_planning_by
from layout.components.global_info_list import global_info_list
from layout.components.graphs import overview_bar_chart
from config import colors_vwt as colors

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
                     collection="Data", client=client, graph_name="voorraadvormend").get("all", "n.v.t."))),
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

    @app.callback(
        Output(f'pie_chart_overview_{client}', 'figure'),
        [Input(f'week-overview-{client}', 'clickData'),
         Input(f'month-overview-{client}', 'clickData'),
         Input(f'overview-reset-{client}', 'n_clicks')
         ]
    )
    def display_click_data(week_click_data, month_click_data, reset, client=client):
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
