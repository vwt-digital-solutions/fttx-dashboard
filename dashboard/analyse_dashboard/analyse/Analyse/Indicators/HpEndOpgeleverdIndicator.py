import copy

import business_rules as br
from Analyse.Indicators.RatioPartIndicator import RatioPartIndicator


class HpEndOpgeleverdIndicator(RatioPartIndicator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.graph_name = "HPend_opgeleverd"

    def apply_business_rules(self):
        df = copy.deepcopy(self.df)
        df["indicator"] = br.hpend(df)
        df = df[["indicator", "project", "opleverdatum"]]
        return df
