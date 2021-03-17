import pandas as pd
from Analyse.Indicators.LineIndicator import LineIndicator
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Capacity_analysis.Domain import DateDomain
from datetime import timedelta


class InternalTargetIndicator(LineIndicator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.type_start_date = 'FTU0'
        self.type_end_date = 'FTU1'
        self.type_total_amount = 'huisaansluitingen'
        self.indicator_name = 'InternalTargetLine'

    def _make_project_line(self, project):
        start_project = self.project_info[project][self.type_start_date]
        end_project = self.project_info[project][self.type_end_date]
        total_amount = self.project_info[project][self.type_total_amount]
        if start_project and end_project and total_amount:
            slope = total_amount / (pd.to_datetime(end_project) - pd.to_datetime(start_project)).days
            domain = DateDomain(begin=start_project, end=pd.to_datetime(end_project) - timedelta(days=1))
            line = TimeseriesLine(data=slope,
                                  domain=domain,
                                  name=self.indicator_name,
                                  max_value=total_amount,
                                  project=project)
        else:
            line = None

        return line
