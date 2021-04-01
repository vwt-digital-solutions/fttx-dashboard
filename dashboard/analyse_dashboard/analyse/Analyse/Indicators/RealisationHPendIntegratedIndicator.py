from Analyse.Indicators.RealisationHPendIndicator import \
    RealisationHPendIndicator
from Analyse.Record.LineRecord import LineRecord


class RealisationHPendIntegratedIndicator(RealisationHPendIndicator):
    """
    Extension of RealisationHPend indicator, as the integrated line is also needed in the front-end.
    """

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
