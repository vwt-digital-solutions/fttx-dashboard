import copy

import business_rules as br
from Analyse.Indicators.ActualRedenNAIndicator import ActualRedenNAIndicator


class ActualRedenNAHCopenOnTimeIndicator(ActualRedenNAIndicator):
    def __init__(self, **kwargs):
        """
        Indicator that creates cluster reden na's for on time open connections.
        Used for modal download gegevens redenna.
        Args:
            **kwargs:
        """
        super().__init__(**kwargs)
        self.collection = "Indicators"
        self.graph_name = "RedenNA_on_time_hc_aanleg"

    def apply_business_rules(self):
        df = copy.deepcopy(self.df)
        df = df[br.hc_aanleg_tmobile(self.df, time_window="on time")]
        return df[["project", "cluster_redenna", "sleutel"]]
