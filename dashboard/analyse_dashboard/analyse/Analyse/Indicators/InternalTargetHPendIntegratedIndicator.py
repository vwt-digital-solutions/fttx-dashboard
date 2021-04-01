from Analyse.Indicators.InternalTargetHPendIndicator import \
    InternalTargetHPendIndicator
from Analyse.Record.LineRecord import LineRecord


class InternalTargetHPendIntegratedIndicator(InternalTargetHPendIndicator):
    """
    Extension of InternalTargethpend indicator, as the integrated line is also needed in the front-end.
    """

    def to_record(self, line):
        return LineRecord(
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
