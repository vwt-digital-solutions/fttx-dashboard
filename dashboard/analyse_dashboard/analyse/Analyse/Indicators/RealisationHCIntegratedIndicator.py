from Analyse.Indicators.RealisationHCIndicator import RealisationHCIndicator
from Analyse.Record.LineRecord import LineRecord


class RealisationHCIntegratedIndicator(RealisationHCIndicator):
    """
    Extension of RealisationHPend indicator, as the integrated line is also needed in the front-end.
    Used for HC/HPend (project)
    """

    def __init__(self, project_info, return_lines=False, **kwargs):
        super().__init__(project_info, return_lines, **kwargs)
        self.indicator_name = "RealisationHCIndicatorIntegrated"

    def to_record(self, line):
        if line:
            record = LineRecord(
                record=line,
                collection="Indicators",
                graph_name=f"{line.name}",
                phase="oplever",
                client=self.client,
                project=line.project,
                to_be_integrated=True,
                to_be_normalized=True,
                to_be_splitted_by_year=True,
                percentage=True,
            )
        else:
            record = None
        return record
