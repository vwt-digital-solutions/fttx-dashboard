import copy

from Analyse.Indicators.PlanningIndicatorKPN import PlanningIndicatorKPN


class PlanningHPCivielIndicatorKPN(PlanningIndicatorKPN):
    """
    calculates the number of houses per day that are planned for
    hpciviel over the complete period of the project.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.indicator_name = "PlanningHPcivielIndicator"

    def apply_business_rules(self):
        """
        Only the 'hp civiel' column is needed

        Returns: Sliced dataframe with only relevant columns.

        """
        df = copy.deepcopy(self.df)
        df = df[["hp civiel"]]
        return df
