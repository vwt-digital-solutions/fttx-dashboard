from app import app
from dash.dependencies import Input, Output

from layout.pages.tmobile import project_view


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
