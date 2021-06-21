import copy

import business_rules as br
from Analyse.Indicators.ActualIndicator import ActualIndicator


class WerkvoorraadLBIndicator(ActualIndicator):
    def __init__(self, **kwargs):
        """
        Indicator to calculate current werkvoorraad for LB
        Used for indicator
        - werkvoorraad Activatie
        - werkvoorraad in FC LB
        """
        super().__init__(**kwargs)
        self.graph_name = "WerkvoorraadLBIndicator"

    def apply_business_rules(self):
        """
        Slice all rows that are currently in werkvoorraad, and retrieve
        the relevant columns to group on.

        Returns: DataFrame with relevant rows and columns.

        """
        df = copy.deepcopy(self.df)
        df["werkvoorraad"] = br.mask_werkvoorraad_activatie_lb_FC(df)
        return df[["project", "werkvoorraad"]]
