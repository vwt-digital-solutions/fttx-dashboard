import copy

import business_rules as br
from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.TimeConstraintIndicator import TimeConstraintIndicator


class HCOpen(TimeConstraintIndicator, Aggregator):
    def __init__(self, **kwargs):
        """
        Indicator to calculate current werkvoorraad
        """
        super().__init__(**kwargs)
        self.graph_name = "HCOpen"

    def apply_business_rules(self):
        df = copy.deepcopy(self.df)
        df["OnTime"] = br.openstaande_orders_tmobile(self.df, time_window="on time")
        df["Late"] = br.openstaande_orders_tmobile(self.df, time_window="limited")
        df["TooLate"] = br.openstaande_orders_tmobile(self.df, time_window="late")
        return df[["project", "OnTime", "Late", "TooLate"]]