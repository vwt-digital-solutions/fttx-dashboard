import copy

import business_rules as br
from Analyse.Indicators.RatioPartIndicator import RatioPartIndicator


class ConnectedOnTimeOrdersTmobile(RatioPartIndicator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.graph_name = "op_tijd_aangesloten_orders_tmobile"

    def apply_business_rules(self):
        """
        HC and HPend columns are needed, as we will calculate ratio between these two columns.
        Opleverdatum and project columns are used for aggregations.

        Returns: Sliced dataframe with only relevant columns.

        """
        df = copy.deepcopy(self.df)
        df["indicator"] = br.aangesloten_orders_tmobile(df, time_window="on time")
        df = df[br.aangesloten_orders_tmobile(df)]
        df = df[["indicator", "project", "opleverdatum"]]
        return df
