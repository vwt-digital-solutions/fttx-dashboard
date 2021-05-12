from Analyse.Indicators.RealisationHCTmobileOnTimeIndicator import \
    RealisationHCTmobileOnTimeIndicator
from Analyse.Record.LineRecord import LineRecord


class RealisationHCIntegratedTmobileOnTimeIndicator(
    RealisationHCTmobileOnTimeIndicator
):
    def __init__(self, project_info, return_lines=False, **kwargs):
        super().__init__(project_info, return_lines, **kwargs)
        self.indicator_name = "RealisationHCIntegratedTmobileOnTimeIndicator"

    def to_record(self, line):
        if line:
            record = LineRecord(
                record=line,
                collection="Indicators",
                graph_name=self.indicator_name,
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
