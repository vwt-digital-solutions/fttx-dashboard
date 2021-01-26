from urllib.parse import urlencode

import dash
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from layout.components.graphs import pie_chart, completed_status_counts_bar
from app import app

import config
from data import collection
from data.data import completed_status_counts, redenna_by_completed_status, \
    fetch_data_for_overview_graphs, no_graph
from layout.components.global_info_list import global_info_list
from layout.components.graphs import overview_bar_chart
from config import colors_vwt as colors
from layout.components import redenna_status_pie
from datetime import datetime

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
            Input(f'{client}-overview', 'style')
        ]
    )
    def load_dropdown(dummy_data, client=client):
        return [collection.get_document(collection="Data",
                                        client=client,
                                        graph_name="project_names")['filters']]

    @app.callback(
        [
            Output(f'year-dropdown-{client}', 'options'),
            Output(f'year-dropdown-{client}', 'value')
        ],
        [
            Input(f'{client}-overview', 'style'),
            Input(f'overview-reset-{client}', 'n_clicks')
        ]
    )
    def load_year_dropdown(dummy_data, reset, client=client):
        return [
            [
                {'label': year, 'value': year} for year in
                collection.get_document(collection="Data", client=client, graph_name="List_of_years")
            ],
            str(datetime.now().year)
        ]

    @app.callback(
        Output(f'month-overview-year-{client}', 'figure'),
        [Input(f'year-dropdown-{client}', 'value')
         ]
    )
    def load_month_overview_per_year(year, client=client):
        if year:
            return overview_bar_chart.get_fig(
                data=fetch_data_for_overview_graphs(year=year, freq='M', period='month', client=client),
                year=year)
        raise PreventUpdate

    @app.callback(
        Output(f'week-overview-year-{client}', 'figure'),
        [Input(f'year-dropdown-{client}', 'value')
         ]
    )
    def load_week_overview_per_year(year, client=client):
        if year:
            return overview_bar_chart.get_fig(
                data=fetch_data_for_overview_graphs(year=year, freq='W-MON', period='week', client=client),
                year=year)
        raise PreventUpdate

    @app.callback(
        Output(f'pie_chart_overview-year_{client}', 'figure'),
        [Input(f'week-overview-year-{client}', 'clickData'),
         Input(f'month-overview-year-{client}', 'clickData'),
         Input(f'overview-reset-{client}', 'n_clicks'),
         Input(f'year-dropdown-{client}', 'value')
         ]
    )
    def display_click_data_per_year(week_click_data, month_click_data, reset, year, client=client):
        '''
        This function returns the "Opgegeven reden na" pie chart, based on what the user has clicked on.
        If no input is given, an annual overview is returned. With input, a monthly or weekly view is returned.

        :return: This function returns a pie chart figure.
        '''
        ctx = dash.callback_context

        if not ctx.triggered:
            return no_graph(title="Opgegeven reden na", text='Loading...')

        last_day_of_period, period, title_text = get_lastdayofperiod_and_titletext(ctx, year)

        if not last_day_of_period and not title_text:
            return no_graph(title="Opgegeven reden na", text='Loading...')

        redenna_by_period = collection.get_document(collection="Data",
                                                    client=client,
                                                    graph_name=f"redenna_by_{period}")
        # Sorted the cluster redenna dict here, so that the pie chart pieces have the proper color:
        redenna_dict = dict(sorted(redenna_by_period.get(last_day_of_period, dict()).items()))

        if redenna_dict:
            return pie_chart.get_html(labels=list(redenna_dict.keys()),
                                      values=list(redenna_dict.values()),
                                      title=title_text,
                                      colors=[
                                         colors['green'],
                                         colors['yellow'],
                                         colors['red'],
                                         colors['vwt_blue'],
                                     ])
        else:
            return no_graph(title=title_text, text='No Data')

    def get_lastdayofperiod_and_titletext(ctx, year):
        '''
        This function returns the settings to plot a pie chart based on annual, monthly or weekly views.

        :param ctx: A dash callback, triggered by clicking in Jaaroverzicht or Maandoverzicht graphs
        :param year: The current year, as set by the year selector dropdown
        :return: last_day_of_period, period, title_text
        '''
        last_day_of_period = ""
        period = ""
        dutch_month_list = ['jan', 'feb', 'maa', 'apr', 'mei', 'jun', 'jul', 'aug', 'sep', 'okt', 'nov', 'dec']

        for trigger in ctx.triggered:
            period, _, _ = trigger['prop_id'].partition("-")

            if period == "overview":
                last_day_of_period = None
                title_text = None
                break
            if period == 'year':
                last_day_of_period = f"{year}-12-31"
                title_text = f"Reden na voor het jaar {year}"
                break
            for point in trigger['value']['points']:
                last_day_of_period = point['customdata']
                if period == 'week':
                    title_text = f"Reden na voor de week {last_day_of_period}"
                if period == 'month':
                    extract_month_in_dutch = dutch_month_list[int(last_day_of_period.split("-")[1]) - 1]
                    title_text = f"Reden na voor de maand {extract_month_in_dutch} {year}"
                break
            break
        return last_day_of_period, period, title_text

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
            if status_counts is None:
                return [no_graph(), no_graph(), {'display': 'block'}, {'display': 'block'}]
            laagbouw = completed_status_counts_bar.get_fig(status_counts.laagbouw,
                                                           title="Status oplevering per fase (LB)")
            laagbouw_style = {'display': 'block'} if laagbouw else {'display': 'none'}
            hoogbouw = completed_status_counts_bar.get_fig(status_counts.hoogbouw,
                                                           title="Status oplevering per fase (HB & Duplex)")
            hoogbouw_style = {'display': 'block'} if hoogbouw else {'display': 'none'}
            return laagbouw, hoogbouw, laagbouw_style, hoogbouw_style
        return {'data': None, 'layout': None}, \
               {'data': None, 'layout': None}, \
               {'display': 'block'}, \
               {'display': 'block'}

    @app.callback(
        [
            Output(f'redenna_project_{client}', 'figure'),
            Output(f'project-redenna-download-{client}', 'href')
        ],
        [
            Input(f'status-count-filter-{client}', 'data'),
            Input(f'project-dropdown-{client}', 'value')
        ]
    )
    def update_redenna_status_clicks(click_filter, project_name, client=client):
        if project_name:
            redenna_counts = redenna_by_completed_status(project_name, click_filter=click_filter, client=client)
            if redenna_counts is None:
                return [no_graph(), ""]
            redenna_pie = redenna_status_pie.get_fig(redenna_counts,
                                                     title="Opgegeven reden na",
                                                     colors=[
                                                        colors['vwt_blue'],
                                                        colors['yellow'],
                                                        colors['red'],
                                                        colors['green']
                                                     ])
            if click_filter:
                download_url = f'/dash/project_redenna_download?project={project_name}&{urlencode(click_filter)}'
            else:
                download_url = f'/dash/project_redenna_download?project={project_name}'

            return [redenna_pie, download_url]
        return [{'data': None, 'layout': None}, ""]

    @app.callback(
        Output(f'info-container-year-{client}', 'children'),
        [Input(f'year-dropdown-{client}', 'value')
         ]
    )
    def load_global_info_per_year(year, client=client):
        if not year:
            raise PreventUpdate

        parameters_global_info_list = [
            dict(id_="info_globaal_container0",
                 title='Target',
                 text=f"HPend afgesproken in {year}: ",
                 value=str(int(collection.get_document(collection="Data",
                                                       graph_name="target",
                                                       client=client,
                                                       year=year,
                                                       frequency="Y")))
                 ),
            dict(id_="info_globaal_container1",
                 title='Realisatie (HPend)',
                 text=f"HPend gerealiseerd in {year}: ",
                 value=str(collection.get_document(collection="Data",
                                                   graph_name="realisatie_hpend",
                                                   client=client,
                                                   year=year,
                                                   frequency="Y"))
                 ),
            dict(id_="info_globaal_container1",
                 title='Realisatie (BIS)',
                 text=f"BIS gerealiseerd in {year}: ",
                 value=str(collection.get_document(collection="Data",
                                                   graph_name="realisatie_bis",
                                                   client=client,
                                                   year=year,
                                                   frequency="Y"))
                 ),
            dict(id_="info_globaal_container2",
                 title='Planning (VWT)',
                 text="HPend gepland vanaf nu: ",
                 value=str(int(collection.get_document(collection="Data",
                                                       graph_name="planning_minus_HPend",
                                                       client=client,
                                                       year=year,
                                                       frequency="Y")))
                 if year == str(datetime.now().year) else 'n.v.t.'  # We only show planning for the current year
                 ),
            dict(id_="info_globaal_container3",
                 title='Voorspelling (VQD)',
                 text="HPend voorspeld vanaf nu: ",
                 value=str(int(collection.get_document(collection="Data",
                                                       graph_name="voorspelling_minus_HPend",
                                                       client=client,
                                                       year=year,
                                                       frequency="Y")))
                 if client != 'tmobile' and year == str(datetime.now().year) else 'n.v.t.'
                 # We only show voorspelling for the current year and only for KPN and DFN
                 ),
            dict(id_="info_globaal_container5",
                 title='Werkvoorraad HAS',
                 text=f"Werkvoorraad HAS in {year}: ",
                 value=str(collection.get_document(collection="Data",
                                                   graph_name="werkvoorraad_has",
                                                   client=client,
                                                   year=year,
                                                   frequency="Y"))
                 ),
            dict(id_="info_globaal_container4",
                 title='Actuele HC / HPend',
                 text=f"HC/HPend in {year}: ",
                 value=str(format(collection.get_document(collection="Data",
                                                          graph_name="ratio_hc_hpend",
                                                          client=client,
                                                          year=year,
                                                          frequency="Y"), '.2f'))
                 if client != 'tmobile' else 'n.v.t.'  # We only show HC/HPend for KPN and DFN
                 ),
            dict(id_="info_globaal_container4",
                 title='Ratio <8 weken',
                 text=f"Ratio <8 weken in {year}: ",
                 value=str(format(collection.get_document(collection="Data",
                                                          graph_name="ratio_8weeks_hpend",
                                                          client=client,
                                                          year=year,
                                                          frequency="Y"), '.2f'))
                 if client == 'tmobile' else 'n.v.t.'  # We only show Ratio <8 weeks for tmobile
                 ),
        ]
        return [
            global_info_list(items=parameters_global_info_list,
                             className="container-display")
        ]
