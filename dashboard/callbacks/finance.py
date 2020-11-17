from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
import pandas as pd
import config
from app import app
from data import collection
from data.data import no_graph

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
            Output(f'budget-bar-{client}', 'figure')
        ],
        [
            Input(f'financial-data-{client}', 'data')
        ],
        [
            State(f'project-dropdown-{client}', 'value')
        ]
    )
    def budget_barchart(data, project, client=client):
        if data:
            budget_df = pd.DataFrame(data.get("budget"))
            categorie_df = budget_df[['categorie', 'kostenbedrag']].groupby("categorie").sum().reset_index()
            expected_actuals_df = pd.DataFrame(data.get("expected_actuals"))
            expected_actuals_df = expected_actuals_df[['categorie', 'kostenbedrag']].groupby(
                "categorie").sum().reset_index()
            actuals_df = pd.DataFrame(data.get("actuals_aggregated"))
            actuals_df = actuals_df[['categorie', 'kostenbedrag']].groupby("categorie").sum().reset_index()

            progress = collection.get_document("Data", client=client, project=project, data_set="progress")
            n_houses = float(progress.get("totaal"))
            progress_percent = {
                k if k != "hpend" else "has": float(v) / n_houses
                for k, v in progress.items()
                if k in ['schouwen', 'montage', 'civiel', 'hpend']
            }
            assumed_expenses_df = (expected_actuals_df.set_index("categorie").kostenbedrag * pd.Series(
                progress_percent)).dropna().to_frame().reset_index()

            assumed_expenses_df.columns = ['categorie', 'kostenbedrag']
            fig = go.Figure(
                [
                    go.Bar(
                        name="Begroting",
                        x=categorie_df.categorie,
                        y=categorie_df.kostenbedrag,
                        marker_color=config.colors_vwt['vwt_blue']
                    ),
                    go.Bar(
                        name="Prognose einde werk",
                        x=expected_actuals_df.categorie,
                        y=expected_actuals_df.kostenbedrag,
                        marker_color=config.colors_vwt['red']
                    ),
                    go.Bar(
                        name="Realisatie",
                        x=actuals_df.categorie,
                        y=actuals_df.kostenbedrag,
                        marker_color=config.colors_vwt['green']
                    ),
                    go.Bar(
                        name="Productie",
                        x=assumed_expenses_df.categorie,
                        y=assumed_expenses_df.kostenbedrag,
                        marker_color=config.colors_vwt['lightgreen']
                    )
                ]
            )
            fig.update_layout(
                height=500,
                paper_bgcolor=config.colors_vwt['paper_bgcolor'],
                plot_bgcolor=config.colors_vwt['plot_bgcolor'],
            )
            return [fig]
        return [no_graph("Barchart")]
