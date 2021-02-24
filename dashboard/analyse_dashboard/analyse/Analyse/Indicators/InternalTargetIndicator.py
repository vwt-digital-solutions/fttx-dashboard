import pandas as pd
from Analyse.Indicators.LineIndicator import LineIndicator
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Capacity_analysis.Domain import DateDomain
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


class InternalTargetIndicator(LineIndicator):

    def transform_dates_to_line(self, project):
        if (self.dates['date_FTU0'][project] is not None) & \
           (self.dates['date_FTU1'][project] is not None) & \
           (self.totals[project] is not None):
            slope = self.totals[project] / (pd.to_datetime(self.dates['date_FTU1'][project]) -
                                            pd.to_datetime(self.dates['date_FTU0'][project])).days
            domain = DateDomain(begin=self.dates['date_FTU0'][project], end=self.dates['date_FTU1'][project])
        else:
            slope = 0
            domain = None
        line = TimeseriesLine(data=slope, domain=domain, name='InternalTargetLine', max_value=self.totals[project])
        return line

    def perform(self):
        record_list = RecordList()
        for project in self.dates['date_FTU0']:
            line = self.transform_dates_to_line(project)
            record_list.append(self.to_record(line, project))
        return record_list

    def to_record(self, line, project):
        return LineRecord(record=line,
                          collection='Lines',
                          graph_name=f'{line.name}',
                          phase='oplever',
                          client=self.client,
                          project=project)
