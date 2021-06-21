import copy

import business_rules as br
from Analyse.Indicators.ActualRedenNAIndicator import ActualRedenNAIndicator


class ActualRedenNAHCopenLateIndicator(ActualRedenNAIndicator):
    def __init__(self, **kwargs):
        """
        Indicator that creates cluster reden na's for late open connections.
        Used for modal download gegevens redenna.
        Args:
            **kwargs:
        """
        super().__init__(**kwargs)
        self.collection = "Indicators"
        self.graph_name = "RedenNA_late_hc_aanleg"

    def apply_business_rules(self):
        df = copy.deepcopy(self.df)
        df = df[br.hc_aanleg_tmobile(self.df, time_window="limited")]
        return df[["project", "cluster_redenna", "sleutel"]]
