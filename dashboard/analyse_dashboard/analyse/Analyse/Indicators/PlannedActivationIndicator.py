from Analyse.Indicators.RealisationIndicator import RealisationIndicator


class PlannedActivationIndicator(RealisationIndicator):
    """
    Indicator to calculate number of houses realised over days per project.
    Makes LineRecords per project, where all relevant details can be calculated.
    Also makes a LineRecords for the aggregate of the project lines for a given client
    """

    def __init__(self, project_info, return_lines=False, **kwargs):
        super().__init__(project_info, return_lines, **kwargs)
        self.type_total_amount = "huisaansluitingen"
        self.columns = ["project", "plandatum"]
        self.indicator_name = "PlannedActivationIndicator"
