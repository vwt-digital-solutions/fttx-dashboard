import copy

import business_rules as br
from Analyse.Indicators.RealisationIndicator import RealisationIndicator


class RealisationHCTmobileIndicator(RealisationIndicator):
    """
    Indicator to calculate number of houses realised over days per project.
    Makes LineRecords per project, where all relevant details can be calculated.
    Also makes a LineRecords for the aggregate of the project lines for a given client
    used for ratio < 12 weken.
    """

    def __init__(self, project_info, return_lines=False, **kwargs):
        super().__init__(project_info, return_lines, **kwargs)
        self.project_info = project_info
        self.return_lines = return_lines
        self.type_total_amount = "huisaansluitingen"
        self.columns = ["project", "opleverdatum"]
        self.indicator_name = "RealisationHCIndicator"

    def apply_business_rules(self):
        """
        HC and HPend columns are needed, as we will calculate ratio between these two columns.
        Opleverdatum and project columns are used for aggregations.

        Returns: Sliced dataframe with only relevant columns.

        """
        df = copy.deepcopy(self.df)
        df = df[br.aangesloten_orders_tmobile(df)]
        df = df[self.columns]
        return df
