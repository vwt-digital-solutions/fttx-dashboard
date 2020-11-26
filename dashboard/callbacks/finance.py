import dash
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc

import pandas as pd
import config
from app import app
from data import collection
from data.data import no_graph
from layout.components.graphs.grouped_bar_chart import get_fig
import plotly.graph_objects as go

for client in config.client_config.keys():
    @app.callback(
        [
            Output(f'financial-data-{client}', 'data'),
            Output(f'progress-over-time-data-{client}', 'data'),
            Output(f"finance-warnings-{client}", 'children')
        ],
        [
            Input(f'project-dropdown-{client}', 'value')
        ]
    )
    def financial_storage(dropdown_selection, client=client):
        if dropdown_selection:
            warnings = [
                # dbc.Alert("Operationele voortgang voor civiel word berekend o.b.v. het status_civiel veld"
                #           " i.p.v. de opleverstatus", color="info")
            ]
            finances = collection.get_document(collection="Finance",
                                               project=dropdown_selection,
                                               client=client)
            finance_document_names = {
                'expected_actuals': 'Prognose einde werk',
                'budget': 'Begroting',
                'actuals': "Realisatie"
            }
            missing_document_consequence = {
                'expected_actuals': "De begroting word nu als referentie gebruikt."
                                    "De prognose einde werk wordt gelijkgesteld aan de begroting."
            }
            for document, value in finances.items():
                if not value:
                    warnings.append(
                        dbc.Alert(f"{dropdown_selection} heeft geen {finance_document_names[document]}. "
                                  f"{missing_document_consequence.get(document, '')}", color="warning"))

                    if document == "expected_actuals":
                        finances[document] = finances['budget']
            progress_over_time_data = collection.get_document(collection="Data",
                                                              project=dropdown_selection,
                                                              client=client,
                                                              data_set="progress_over_time")
            return [finances, progress_over_time_data, warnings]
        return [None, None, None]

    @app.callback(
        [
            Output(f'budget-bar-category-{client}', 'figure')
        ],
        [
            Input(f'financial-data-{client}', 'data')
        ],
        [
            State(f'project-dropdown-{client}', 'value')
        ]
    )
    def budget_bar_category(data, project, client=client):
        if data:
            fig = calculate_figure(client, project, data, "categorie")
            return [fig]
        return [no_graph("Barchart")]

    @app.callback(
        [
            Output(f'budget-bar-sub-category-{client}', 'figure'),
            Output(f'budget-bar-sub-category-{client}-container-subtitle', 'children')
        ],
        [
            Input(f"budget-bar-category-{client}", 'clickData'),
            Input(f'project-dropdown-{client}', 'value')

        ],
        [
            State(f'financial-data-{client}', 'data')
        ]
    )
    def budget_bar_sub_category(click, project, data, client=client):
        ctx = dash.callback_context
        if 'budget-bar-category' in [x['prop_id'].rpartition("-")[0] for x in ctx.triggered]:
            for point in click.get("points", []):
                print(f"point={point}")
                if data:
                    parent = dict(level='categorie', value=point.get("label"))
                    fig = calculate_figure(client, project, data, "sub_categorie", parent)
                    return [fig, point.get("label")]
                break
        return [no_graph(text="Geen selectie"), ""]

    def calculate_figure(client, project, data, level, parent: dict = None):
        actuals_df, budget_df, expected_actuals_df = calculate_level_costs(data, level, parent=parent)
        assumed_expenses_df = calculate_assumed_expenses(client, project, expected_actuals_df, level, parent)
        fig = get_fig(dict(name="Begroting",
                           x=budget_df[level],
                           y=budget_df.kostenbedrag,
                           color=config.colors_vwt['lightgray']),
                      dict(name="Prognose einde werk",
                           x=expected_actuals_df[level],
                           y=expected_actuals_df.kostenbedrag,
                           color=config.colors_vwt['black']),
                      dict(name="Realisatie",
                           x=actuals_df[level],
                           y=actuals_df.kostenbedrag,
                           color=config.colors_vwt['vwt_blue']),
                      dict(name="Operationeel",
                           x=assumed_expenses_df[level],
                           y=assumed_expenses_df.kostenbedrag,
                           color=config.colors_vwt['darkgray'])
                      )
        return fig

    def calculate_level_costs(data, level, parent: dict = None):
        """
        :param data: The stored budget data in json/dictionary format.
        :param level: The level at which the costs are selected: [categorie, sub_categorie]
        :param parent: A dictionary determining the parent level and value.
                       Example: dict(level='categorie', value='civiel')
        :return: actuals_df, budget_df, expected_actuals_df
        """

        budget_df = pd.DataFrame(data.get("budget"))
        expected_actuals_df = pd.DataFrame(data.get("expected_actuals"))
        actuals_df = pd.DataFrame(data.get("actuals_aggregated"))

        if parent:
            budget_df = budget_df[budget_df[parent.get("level")] == parent.get("value")]
            expected_actuals_df = expected_actuals_df[expected_actuals_df[parent.get("level")] == parent.get("value")]
            actuals_df = actuals_df[actuals_df[parent.get("level")] == parent.get("value")]

        budget_df = budget_df[[level, 'kostenbedrag']].groupby(level).sum().reset_index()
        expected_actuals_df = expected_actuals_df[[level, 'kostenbedrag']].groupby(level).sum().reset_index()
        actuals_df = actuals_df[[level, 'kostenbedrag']].groupby(level).sum().reset_index()
        return actuals_df, budget_df, expected_actuals_df

    def calculate_assumed_expenses(client, project, expected_actuals_df, level, parent: dict = None):
        progress = collection.get_document("Data", client=client, project=project, data_set="progress")
        n_houses = float(progress.get("totaal"))
        if parent:
            parent_value = parent.get("value") if parent.get("value") != "has" else "hpend"
        else:
            parent_value = None
        progress_percent = {
            k if k != "hpend" else "has": float(v) / n_houses
            for k, v in progress.items()
            if k in ([parent_value] if parent else ['schouwen', 'montage', 'civiel', 'hpend'])
        }
        if parent:
            assumed_expenses_df = pd.Series(
                {
                    v[level]: v['kostenbedrag'] * progress_percent.get(
                        "has" if parent_value == "hpend" else parent_value,
                        0
                    )
                    for k, v in expected_actuals_df.iterrows()
                }
            ).dropna().to_frame().reset_index()
        else:
            assumed_expenses_df = (expected_actuals_df.set_index(level).kostenbedrag * pd.Series(
                progress_percent)).dropna().to_frame().reset_index()
        assumed_expenses_df.columns = [level, 'kostenbedrag']
        return assumed_expenses_df

    @app.callback(
        [
            Output(f'progress-over-time-{client}', 'figure'),
            Output(f'progress-over-time-{client}-container-subtitle', 'children')
        ],
        [
            Input(f"budget-bar-category-{client}", 'clickData'),
            Input(f'project-dropdown-{client}', 'value')
        ],
        [
            State(f'financial-data-{client}', 'data'),
            State(f'progress-over-time-data-{client}', 'data')
        ]
    )
    def progress_over_time(click, project, finance_data, progress_data):
        ctx = dash.callback_context
        if 'budget-bar-category' in [x['prop_id'].rpartition("-")[0] for x in ctx.triggered]:
            for point in click.get("points", []):
                if finance_data:
                    parent = dict(level='categorie', value=point.get("label"))
                    actuals_df = pd.DataFrame(finance_data.get('actuals'))
                    actuals_df = actuals_df[actuals_df[parent.get("level")] == parent.get("value")]
                    time_series = actuals_df.groupby("registratiedatum")['kostenbedrag'].sum().sort_index().cumsum()

                    expected_cost = finance_data.get('expected_actuals')
                    expected_cost = pd.DataFrame(expected_cost)
                    expected_cost = expected_cost[
                        expected_cost[parent.get("level")] == parent.get('value')].kostenbedrag.sum()

                    traces = [go.Scatter(
                        x=time_series.index,
                        y=time_series,
                        mode='lines+markers',
                        name="Financieel",
                        line=dict(color=config.colors_vwt['vwt_blue'])
                    )]

                    if parent.get("value") in ["has", "civiel", "montage", "schouwen"]:
                        traces.append(get_progress_scatter(
                            expected_cost,
                            progress_data,
                            parent.get("value"),
                            color=config.colors_vwt['darkgray']
                        ))

                    fig = go.Figure(
                        data=traces
                    )
                    fig.update_layout(
                        height=500,
                        paper_bgcolor=config.colors_vwt['paper_bgcolor'],
                        plot_bgcolor=config.colors_vwt['plot_bgcolor'],
                    )
                    return [fig, parent.get("value")]
                break
        return [no_graph(text="Geen selectie"), ""]

    def get_progress_scatter(expected_cost, progress_data, phase, color=None):
        progress_series = get_progress_series(expected_cost, phase, progress_data)
        scatter = go.Scatter(
            x=progress_series.index,
            y=progress_series,
            mode='lines+markers',
            name="Operationeel",
            line=dict(color=color)
        )
        return scatter

    def get_progress_series(expected_cost, phase, progress_data):
        progress_series = pd.Series(progress_data[phase]) * expected_cost
        progress_series.index = pd.to_datetime(progress_series.index)
        progress_series = progress_series.sort_index()
        return progress_series
