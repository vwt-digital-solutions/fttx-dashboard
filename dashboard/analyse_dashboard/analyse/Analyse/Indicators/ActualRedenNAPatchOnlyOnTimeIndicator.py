import copy

import business_rules as br
from Analyse.Indicators.ActualRedenNAIndicator import ActualRedenNAIndicator


class ActualRedenNAPatchOnlyOnTimeIndicator(ActualRedenNAIndicator):
    def __init__(self, **kwargs):
        """
        Indicator that creates cluster reden na's for on time patch only connections.

        Args:
            **kwargs:
        """
        super().__init__(**kwargs)
        self.collection = "Indicators"
        self.graph_name = "RedenNA_on_time_patch_only"

    def apply_business_rules(self):
        df = copy.deepcopy(self.df)
        df = df[br.hc_patch_only_tmobile(self.df, time_window="on time")]
        return df[["project", "cluster_redenna", "sleutel"]]
