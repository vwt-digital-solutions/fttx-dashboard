import pandas as pd
from Analyse.Indicators.LineIndicator import LineIndicator
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Capacity_analysis.Domain import DateDomain
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


class InternalTargetIndicator(LineIndicator):

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

    def perform(self):
        record_list = RecordList()
        for project in self.project_info:
            line = self.transform_dates_to_line(self.project_info[project])
            record_list.append(self.to_record(line, project))
        return record_list

    def to_record(self, line, project):
        return LineRecord(record=line,
                          collection='Lines',
                          graph_name=f'{line.name}',
                          phase='oplever',
                          client=self.client,
                          project=project)
