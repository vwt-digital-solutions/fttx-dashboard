import copy

import business_rules as br
from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.TimeConstraintIndicator import TimeConstraintIndicator


class HCPatchOnly(TimeConstraintIndicator, Aggregator):
    def __init__(self, **kwargs):
        """
        Indicator to calculate current Patch only amount.
        Used for indicators:
        - patch only open op tijd
        - patch only open laat
        - patch only open te laat
        """
        super().__init__(**kwargs)
        self.graph_name = "PatchOnly"

    def apply_business_rules(self):
        df = copy.deepcopy(self.df)
        df["OnTime"] = br.hc_patch_only_tmobile(self.df, time_window="on time")
        df["Late"] = br.hc_patch_only_tmobile(self.df, time_window="limited")
        df["TooLate"] = br.hc_patch_only_tmobile(self.df, time_window="late")
        return df[["project", "OnTime", "Late", "TooLate"]]
