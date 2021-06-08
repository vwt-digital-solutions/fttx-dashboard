from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

import config
from app import app
from data import collection
from layout.components.graphs.grouped_bar_chart import get_fig

colors = config.colors_vwt

for client in config.client_config.keys():  # noqa: C901

    @app.callback(
        Output(f"graph-actual-connection-type-activatie-{client}", "figure"),
        [Input(f"project-dropdown-{client}", "value")],
    )
    def actual_connection_type(project, client=client):
        if project:
            data = collection.get_document(
                collection="Indicators", project=project, client=client, graph_name='ActualConnectionTypeIndicator'
            )
            if data:
                bar = {'name': 'Actual Connections',
                       'x': list(data.values()),
                       'y': list(data.keys()),
                       'color': colors.get('vwt_blue')}
                fig = get_fig(bar)
                return fig
        raise PreventUpdate
