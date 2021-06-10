import copy

import business_rules as br
from Analyse.Indicators.ActualIndicator import ActualIndicator


class OpenstaandeAanvragenIndicator(ActualIndicator):
    def __init__(self, **kwargs):
        """
        Indicator to calculate current werkvoorraad
        """
        super().__init__(**kwargs)
        self.graph_name = "OpenstaandeAanvragenIndicator"

    def apply_business_rules(self):
        """
        Slice all rows that are currently in werkvoorraad, and retrieve
        the relevant columns to group on.

        Returns: DataFrame with relevant rows and columns.

        """
        df = copy.deepcopy(self.df)
        df["aanvragen_openstaand"] = br.mask_openstaande_aanvragen(df)
        return df[["project", "aanvragen_openstaand"]]
