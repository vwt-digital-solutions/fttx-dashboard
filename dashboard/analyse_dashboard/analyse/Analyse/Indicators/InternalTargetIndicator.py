import pandas as pd
from Analyse.Indicators.LineIndicator import LineIndicator
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Capacity_analysis.Domain import DateDomain
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


class InternalTargetIndicator(LineIndicator):

    def perform(self):
        record_list = RecordList()
        line_client, total_amount_client = self.initialize_line_client(self.project_info)
        for project in self.project_info:
            line = self.transform_dates_to_line(self.project_info[project])
            line_client = line_client.add(line, fill_value=0)
            record_list.append(self.to_record(line, project))
        line_client.name = 'InternalTargetLine'
        line_client.max_value = total_amount_client
        record_list.append(self.to_record(line_client, project='client_aggregate'))

        return record_list

    def initialize_line_client(self, project_info):
        first_FTU0_client = min([dict['FTU0'] for dict in self.project_info.values() if dict['FTU0']])
        last_FTU1_client = max([dict['FTU1'] for dict in self.project_info.values() if dict['FTU1']])
        total_amount_client = sum([dict['huisaansluitingen'] for dict in self.project_info.values()
                                   if dict['huisaansluitingen']])
        line_client = TimeseriesLine(data=0, domain=DateDomain(begin=first_FTU0_client, end=last_FTU1_client))

        return line_client, total_amount_client

    def transform_dates_to_line(self, project_info):
        if (project_info['FTU0'] is not None) & \
           (project_info['FTU1'] is not None) & \
           (project_info['huisaansluitingen'] is not None):
            slope = project_info['huisaansluitingen'] / (pd.to_datetime(project_info['FTU1']) -
                                                         pd.to_datetime(project_info['FTU0'])).days
            domain = DateDomain(begin=project_info['FTU0'], end=project_info['FTU1'])
        else:
            slope = 0
            domain = DateDomain(begin=pd.Timestamp.now(), end=pd.Timestamp.now())
        line = TimeseriesLine(data=slope,
                              domain=domain,
                              name='InternalTargetLine',
                              max_value=project_info['huisaansluitingen'])
        return line

    def to_record(self, line, project):
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
