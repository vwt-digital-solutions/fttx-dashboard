from Analyse.Indicators.RealisationIndicator import RealisationIndicator


class RealisationHPendIndicator(RealisationIndicator):
    """
    Indicator to calculate number of houses realised over days per project.
    Makes LineRecords per project, where all relevant details can be calculated.
    Also makes a LineRecords for the aggregate of the project lines for a given client.
    Used for indicators:
    - Realisatie Connectie HPend (overview)
    - realisatie HP end (project)
    - HC/HPend ratio
    - jaaroverzicht.
    """

    def __init__(self, project_info, return_lines=False, **kwargs):
        super().__init__(project_info, return_lines, **kwargs)
        self.project_info = project_info
        self.return_lines = return_lines
        self.type_total_amount = "huisaansluitingen"
        self.columns = ["project", "opleverdatum"]
        self.indicator_name = "RealisationHPendIndicator"
