import copy

import business_rules as br
from Analyse.Indicators.ActualIndicator import ActualIndicator


class WerkvoorraadLBAssignedIndicator(ActualIndicator):
    def __init__(self, **kwargs):
        """
        Indicator to calculate current werkvoorraad for LB
        """
        super().__init__(**kwargs)
        self.graph_name = "WerkvoorraadLBAssignedIndicator"

    def apply_business_rules(self):
        """
        Slice all rows that are currently in werkvoorraad, and retrieve
        the relevant columns to group on.

        Returns: DataFrame with relevant rows and columns.

        """
        df = copy.deepcopy(self.df)
        df["werkvoorraad"] = br.mask_werkvoorraad_activatie_lb_assigned(df)
        return df[["project", "werkvoorraad"]]
