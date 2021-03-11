import pandas as pd
from Analyse.Indicators.Indicator import Indicator
from Analyse.Capacity_analysis.Domain import DateDomain
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Record.RecordList import RecordList
from Analyse.Record.LineRecord import LineRecord
from datetime import timedelta


class LineIndicator(Indicator):
    """
    Barebones indicator class containing standard functionality that every type of Indicator will be able to do.
    """

    def __init__(self, client, project_info, type_start_date, type_end_date, type_total_amount, df=None):
        self.client = client
        self.project_info = project_info
        self.type_start_date = type_start_date
        self.type_end_date = type_end_date
        self.type_total_amount = type_total_amount
        self.df = df

    def _make_project_lines_from_dates_in_project_info(self):
        lines = []
        for project in self.project_info:
            start_project = self.project_info[project][self.type_start_date]
            end_project = self.project_info[project][self.type_end_date]
            total_amount = self.project_info[project][self.type_total_amount]
            if (start_project is not None) & (end_project is not None) & (total_amount is not None):
                slope = total_amount / (pd.to_datetime(end_project) - pd.to_datetime(start_project)).days
                domain = DateDomain(begin=start_project, end=pd.to_datetime(end_project) - timedelta(days=1))
            else:
                slope = 0
                domain = DateDomain(begin=pd.Timestamp.now(), end=pd.Timestamp.now())
            lines.append(TimeseriesLine(data=slope,
                                        domain=domain,
                                        name='InternalTargetLine',
                                        max_value=total_amount,
                                        project=project))
        return lines

    def _initialize_client_line_aggregate(self):
        info_projects = self.project_info.values()
        first_date_client = min([info_project[self.type_start_date] for info_project in info_projects
                                if info_project[self.type_start_date]])
        last_date_client = max([info_project[self.type_end_date] for info_project in info_projects
                                if info_project[self.type_end_date]])
        domain = DateDomain(begin=first_date_client, end=last_date_client)
        line = TimeseriesLine(data=0, domain=domain)
        return line

    def _add_client_aggregate_line_to_list_of_project_lines(self, list_lines):
        line_client_aggregate = self._initialize_client_line_aggregate()
        for line in list_lines:
            line_client_aggregate = line_client_aggregate.add(line, fill_value=0)
        line_client_aggregate.name = 'InternalTargetLine'
        line_client_aggregate.project = self.client
        list_lines.append(line_client_aggregate)
        return list_lines

    def _make_list_of_records_from_list_of_lines(self, lines):
        list_line_records = RecordList()
        for line in lines:
            list_line_records.append(LineRecord(record=line,
                                                collection='Lines',
                                                graph_name=f'{line.name}',
                                                phase='oplever',
                                                client=self.client,
                                                project=line.project,
                                                to_be_integrated=False,
                                                to_be_normalized=False,
                                                to_be_splitted_by_year=True,
                                                percentage=False))
        return list_line_records
