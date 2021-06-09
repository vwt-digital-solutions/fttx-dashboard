from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate

import config
from app import app
from data import collection
from layout.components.graphs.horizontal_bar_chart import get_fig
import collections

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

            new_dict = dict()
            for key, value in data.items():
                new_dict[int(float(key))] = value

            ordered_dict = collections.OrderedDict(sorted(new_dict.items()))

            if data:
                bar = {'name': 'Actual Connections',
                       'x': list(ordered_dict.values()),
                       'y': list(ordered_dict.keys()),
                       'color': colors.get('vwt_blue')}

                fig = get_fig(bar)
                fig.update_layout(yaxis=dict(type='category'))
                return fig
        raise PreventUpdate
