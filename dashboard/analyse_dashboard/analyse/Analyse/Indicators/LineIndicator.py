from Analyse.Capacity_analysis.Line import concat
from Analyse.Indicators.Indicator import Indicator
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


class LineIndicator(Indicator):
    """
    Barebone indicator class containing standard functionality that every type of Indicator will be able to do.
    """

    def __init__(self, client, project_info):
        self.df = None
        super().__init__(df=self.df, client=client)
        self.project_info = project_info
        self.type_start_date = None
        self.type_end_date = None
        self.type_total_amount = None

    def perform(self):
        """Adds for each project a record to the record_list that contains the line made at _make_project_line.

        Returns:
            List of records which are ready for sending to the firestore.
        """
        line_list = []
        record_list = RecordList()
        for project in self.project_info:
            line_project = self._make_project_line(project)
            if line_project:
                line_list.append(line_project)
                record_list.append(self.to_record(line_project))
        if line_list:
            line_client = concat(
                line_list, name=self.indicator_name, project="client_aggregate"
            )
            line_list.append(line_client)
            record_list.append(self.to_record(line_client))
        return record_list

    def _make_project_line(self):
        return None

    def to_record(self, line):
        return LineRecord(
            record=line,
            collection="Indicators",
            graph_name=f"{line.name}",
            phase="oplever",
            client=self.client,
            project=line.project,
            to_be_integrated=False,
            to_be_normalized=False,
            to_be_splitted_by_year=True,
            percentage=False,
        )
