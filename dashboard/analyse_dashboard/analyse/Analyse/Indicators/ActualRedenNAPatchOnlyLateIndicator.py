import copy

import business_rules as br
from Analyse.Indicators.ActualRedenNAIndicator import ActualRedenNAIndicator


class ActualRedenNAPatchOnlyLateIndicator(ActualRedenNAIndicator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.collection = "Indicators"
        self.graph_name = "RedenNA_late_patch_only"

    def apply_business_rules(self):
        df = copy.deepcopy(self.df)
        df = df[br.hc_patch_only_tmobile(self.df, time_window="limited")]
        return df[["project", "cluster_redenna", "sleutel"]]
