import dash
from app import app
from dash.dependencies import Input, Output

from data import collection
from layout.components.graphs import pie_chart
from layout.pages.tmobile import project_view
from data.graph import pie_chart as original_pie_chart


@app.callback(
    [
        Output(component_id="tmobile-overview", component_property='style')
    ],
    [
        Input('project-dropdown-tmobile', 'value'),
    ],
)
def tmobile_overview(dropdown_selection):
    if dropdown_selection:
        return [{'display': 'none'}]
    return [{'display': 'block'}]


@app.callback(
    [
        Output(component_id="tmobile-project-view", component_property='style'),
        Output("tmobile-project-view", "children"),
    ],
    [
        Input('project-dropdown-tmobile', 'value'),
    ],
)
def tmobile_project_view(dropdown_selection):
    if dropdown_selection:
        return [{'display': 'block'}, project_view.get_html(dropdown_selection)]
    return [{'display': 'none'}, project_view.get_html(dropdown_selection)]


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
    Output('redenna_by_week', 'figure'),
    [Input('week-overview', 'clickData'),
     Input('month-overview', 'clickData')
     ]
)
def display_click_data(week_click_data, month_click_data):
    ctx = dash.callback_context
    first_day_of_period = ""
    period = ""
    if ctx.triggered:
        for trigger in ctx.triggered:
            period, _, _ = trigger['prop_id'].partition("-")
            for point in trigger['value']['points']:
                first_day_of_period = point['label']
                break
            break

        redenna_by_period = collection.get_document(collection="Data",
                                                    client="t-mobile",
                                                    graph_name=f"redenna_by_{period}")

        fig = pie_chart.get_html(labels=list(redenna_by_period.get(first_day_of_period, dict()).keys()),
                                 values=list(redenna_by_period.get(first_day_of_period, dict()).values()),
                                 title=f"Reden na voor de {period} {first_day_of_period}")

        return fig
    return original_pie_chart('t-mobile')
