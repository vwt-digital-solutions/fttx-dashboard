import copy

import business_rules as br
from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.TimeConstraintIndicator import TimeConstraintIndicator


class HCOpen(TimeConstraintIndicator, Aggregator):
    def __init__(self, **kwargs):
        """
        Indicator to calculate current werkvoorraad
        used for indicator:
        - HC open op tijd
        - HC open laat
        - HC open te laat
        """
        super().__init__(**kwargs)
        self.graph_name = "HCopen"

    def apply_business_rules(self):
        df = copy.deepcopy(self.df)
        df["OnTime"] = br.hc_aanleg_tmobile(self.df, time_window="on time")
        df["Late"] = br.hc_aanleg_tmobile(self.df, time_window="limited")
        df["TooLate"] = br.hc_aanleg_tmobile(self.df, time_window="late")
        return df[["project", "OnTime", "Late", "TooLate"]]
