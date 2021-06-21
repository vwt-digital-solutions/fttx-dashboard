import copy

import business_rules as br
from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.ActualIndicator import ActualIndicator


class WerkvoorraadIndicator(ActualIndicator, Aggregator):
    def __init__(self, **kwargs):
        """
        Indicator to calculate current werkvoorraad
        Used for indicator
        - werkvoorraad Connectie
        - werkvoorraad HAS (tmobile)
        """
        super().__init__(**kwargs)
        self.graph_name = "WerkvoorraadHPendIndicator"

    def apply_business_rules(self):
        """
        Slice all rows that are currently in werkvoorraad, and retrieve
        the relevant columns to group on.

        Returns: DataFrame with relevant rows and columns.

        """
        df = copy.deepcopy(self.df)
        df["werkvoorraad"] = br.has_werkvoorraad(df)
        return df[["project", "werkvoorraad"]]
