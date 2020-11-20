from dash.dependencies import Input, Output, State
import pandas as pd
import config
from app import app
from data import collection
from data.data import no_graph
from layout.components.graphs.grouped_bar_chart import get_fig

for client in config.client_config.keys():
    @app.callback(
        [
            Output(f'financial-data-{client}', 'data')
        ],
        [
            Input(f'project-dropdown-{client}', 'value')
        ]
    )
    def financial_storage(dropdown_selection, client=client):
        if dropdown_selection:
            finances = collection.get_document(collection="Finance",
                                               project=dropdown_selection,
                                               client=client)
            if finances:
                return [finances]
        return [None]

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
            Output(f'budget-bar-sub-category-{client}', 'figure')
        ],
        [
            Input(f"budget-bar-category-{client}", 'clickData')
        ],
        [
            State(f'financial-data-{client}', 'data'),
            State(f'project-dropdown-{client}', 'value')
        ]
    )
    def budget_bar_sub_category(click, data, project, client=client):
        for point in click.get("points", []):
            if data:
                parent = dict(level='categorie', value=point.get("label"))
                fig = calculate_figure(client, project, data, "sub_categorie", parent)
                return [fig]
            break
        return [no_graph("Barchart", "Geen selectie")]

    def calculate_figure(client, project, data, level, parent: dict = None):
        actuals_df, budget_df, expected_actuals_df = calculate_level_costs(data, level, parent=parent)
        assumed_expenses_df = calculate_assumed_expenses(client, project, expected_actuals_df, level, parent)
        fig = get_fig(dict(name="Begroting",
                           x=budget_df[level],
                           y=budget_df.kostenbedrag,
                           color=config.colors_vwt['vwt_blue']),
                      dict(name="Prognose einde werk",
                           x=expected_actuals_df[level],
                           y=expected_actuals_df.kostenbedrag,
                           color=config.colors_vwt['red']),
                      dict(name="Realisatie",
                           x=actuals_df[level],
                           y=actuals_df.kostenbedrag,
                           color=config.colors_vwt['green']),
                      dict(name="Productie",
                           x=assumed_expenses_df[level],
                           y=assumed_expenses_df.kostenbedrag,
                           color=config.colors_vwt['lightgreen'])
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
