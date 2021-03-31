import copy

from Analyse.Indicators.PlanningIndicatorKPN import PlanningIndicatorKPN


class PlanningHPEndIndicatorKPN(PlanningIndicatorKPN):
    """
    Calculates the HPEnd planning for KPN projects
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.indicator_name = "PlanningHPendIndicatorKPN"

    def apply_business_rules(self):
        """
        Only the 'hp end' column is needed

        Returns: Sliced dataframe with only relevant columns.

        """
        df = copy.deepcopy(self.df)
        df = df[["hp end"]]
        return df
