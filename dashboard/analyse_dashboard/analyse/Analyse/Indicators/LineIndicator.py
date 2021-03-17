from Analyse.Indicators.Indicator import Indicator
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


class LineIndicator(Indicator):
    """
    Barebones indicator class containing standard functionality that every type of Indicator will be able to do.
    """

    def __init__(self, client, project_info):
        self.client = client
        self.project_info = project_info
        self.type_start_date = None
        self.type_end_date = None
        self.type_total_amount = None
        self.indicator_name = None
        self.df = None

    def perform(self):
        record_list = RecordList()
        line_client_aggregate = None
        for project in self.project_info:
            line_project = self._make_project_line(project)
            self._add_line_to_list_of_records(line_project, record_list)
            line_client_aggregate = self._add_line_to_line_client_aggregate(line_project, line_client_aggregate)
        self._add_line_to_list_of_records(line_client_aggregate, record_list)
        return record_list

    def _make_project_line(self):
        return None

    def _add_line_to_line_client_aggregate(self, line, line_client=None):
        if line_client and line:
            line_client = line_client.add(line, fill_value=0)
        elif line:
            line_client = line
        if line_client:
            line_client.name = self.indicator_name
            line_client.project = self.client
        return line_client

    def _add_line_to_list_of_records(self, line, record_list):
        if line:
            record_list.append(LineRecord(record=line,
                                          collection='Lines',
                                          graph_name=f'{line.name}',
                                          phase='oplever',
                                          client=self.client,
                                          project=line.project,
                                          to_be_integrated=False,
                                          to_be_normalized=False,
                                          to_be_splitted_by_year=True,
                                          percentage=False))
