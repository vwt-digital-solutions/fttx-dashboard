from Analyse.Indicators.PlannedActivationIndicator import \
    PlannedActivationIndicator
from Analyse.Record.LineRecord import LineRecord


class PlannedActivationIntegratedIndicator(PlannedActivationIndicator):
    """
    Indicator to calculate number of houses realised over days per project.
    Makes LineRecords per project, where all relevant details can be calculated.
    Also makes a LineRecords for the aggregate of the project lines for a given client
    used for indicators: voortgang tijdseries
    """

    def __init__(self, project_info=None, return_lines=False, **kwargs):
        super().__init__(project_info, return_lines, **kwargs)
        self.indicator_name = "PlannedActivationIntegratedIndicator"

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
                to_be_normalized=False,
                to_be_splitted_by_year=True,
                percentage=False,
            )
        else:
            record = None
        return record
