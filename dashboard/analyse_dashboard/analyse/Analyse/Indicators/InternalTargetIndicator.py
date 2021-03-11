import pandas as pd
from datetime import timedelta
from Analyse.Indicators.LineIndicator import LineIndicator
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Capacity_analysis.Domain import DateDomain
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


class InternalTargetIndicator(LineIndicator):

    def perform(self):
        list_line_records = RecordList()
        line_client_aggregate = self.initialize_line_client_aggregate()
        for project in self.project_info:
            line_project = self.make_line_from_project_dates(project)
            list_line_records.append(self.make_record_from_line(line_project, project))
            line_client_aggregate = line_client_aggregate.add(line_project, fill_value=0)
        line_client_aggregate.name = 'InternalTargetLine'
        list_line_records.append(self.make_record_from_line(line_client_aggregate, project='client_aggregate'))

        return list_line_records

    def initialize_line_client_aggregate(self):
        first_FTU0_client = min([project['FTU0'] for project in self.project_info.values() if project['FTU0']])
        last_FTU1_client = max([project['FTU1'] for project in self.project_info.values() if project['FTU1']])
        domain = DateDomain(begin=first_FTU0_client, end=last_FTU1_client)
        line = TimeseriesLine(data=0, domain=domain)
        return line

    def make_line_from_project_dates(self, project):
        start_project = self.project_info[project]['FTU0']
        end_project = self.project_info[project]['FTU1']
        total_amount = self.project_info[project]['huisaansluitingen']
        if (start_project is not None) & (end_project is not None) & (total_amount is not None):
            slope = total_amount / (pd.to_datetime(end_project) - pd.to_datetime(start_project)).days
            domain = DateDomain(begin=start_project, end=pd.to_datetime(end_project) - timedelta(days=1))
        else:
            slope = 0
            domain = DateDomain(begin=pd.Timestamp.now(), end=pd.Timestamp.now())
        line = TimeseriesLine(data=slope, domain=domain, name='InternalTargetLine', max_value=total_amount)
        return line

    def make_record_from_line(self, line, project):
        return LineRecord(record=line,
                          collection='Lines',
                          graph_name=f'{line.name}',
                          phase='oplever',
                          client=self.client,
                          project=project,
                          to_be_integrated=False,
                          to_be_normalized=False,
                          to_be_splitted_by_year=True,
                          percentage=False)
