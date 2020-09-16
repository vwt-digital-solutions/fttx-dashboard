import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc

from layout.components.header import header
from layout.pages.tmobile import overview, project_view
from data import collection

layout = dict(
    autosize=True,
    automargin=True,
    margin=dict(le=30, r=30, b=20, t=40),
    hovermode="closest",
    plot_bgcolor="#F9F9F9",
    paper_bgcolor="#F9F9F9",
    legend=dict(font=dict(size=10), orientation="h"),
)


# APP LAYOUT
def get_body():
    page = html.Div(
        [
            dcc.Store(id="project_filter_tmobile"),
            dcc.Store(id="aggregate_data",
                      data=None),
            dcc.Store(id="aggregate_data2",
                      data=None),
            dcc.Store(id="aggregate_data3",
                      data=None),
            header("Status projecten T-Mobile in 2020"),

            html.Div(
                id="tmobile-overview",
                children=overview.get_html(),
            ),
            html.Div(
                [
                    html.Div(
                        [dcc.Dropdown(id='project-dropdown-tmobile',
                                      options=collection.get_document(collection="Data", client="t-mobile",
                                                                      graph_name="project_names")['filters'],
                                      value=None)],
                        className="two-third column",
                    ),
                    html.Div(
                        [dbc.Button('Terug naar overzicht alle projecten', id='overzicht-button-tmobile')],
                        className="one-third column",
                    ),
                ],
                className="container-display",
                id="title",
            ),
            html.Div(
                style={'display': 'none'},
                id="tmobile-project-view",
                children=project_view.get_html(""),
            ),
        ],
        id="mainContainer",
        style={"display": "flex", "flex-direction": "column"},
    )
    return page
