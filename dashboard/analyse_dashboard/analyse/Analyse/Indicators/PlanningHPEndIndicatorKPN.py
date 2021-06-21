import copy

from Analyse.Indicators.PlanningIndicatorKPN import PlanningIndicatorKPN


class PlanningHPEndIndicatorKPN(PlanningIndicatorKPN):
    """
    calculates the number of houses per day that are planned for
    hpend over the complete period of the project.
    used for indicators:
    - planning connectie hpend
    - jaaroverzicht
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.indicator_name = "PlanningHPendIndicator"

    def apply_business_rules(self):
        """
        Only the 'hp end' column is needed

        Returns: Sliced dataframe with only relevant columns.

        """
        df = copy.deepcopy(self.df)
        df = df[["hp end"]]
        return df
