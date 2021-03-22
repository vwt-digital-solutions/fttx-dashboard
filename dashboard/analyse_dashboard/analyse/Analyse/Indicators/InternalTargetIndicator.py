import pandas as pd
import math
from datetime import timedelta
from Analyse.Indicators.LineIndicator import LineIndicator
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Capacity_analysis.Domain import DateDomainRange


class InternalTargetIndicator(LineIndicator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.type_start_date = 'FTU0'
        self.type_end_date = 'FTU1'
        self.n_days_shift_end_date = 0
        self.type_speed = 'snelheid (m/week)'
        self.type_total_houses = 'huisaansluitingen'
        self.type_total_meters = 'meters BIS'
        self.indicator_name = 'InternalTargetLine'

    def _make_project_line(self, project):
        start_project = self.project_info[project][self.type_start_date]
        end_project = self.project_info[project][self.type_end_date]
        if end_project:
            end_project = (pd.to_datetime(end_project) - timedelta(self.n_days_shift_end_date)).strftime('%Y-%m-%d')
        total_houses = self.project_info[project][self.type_total_houses]
        total_meters = self.project_info[project][self.type_total_meters]
        speed_project = self.project_info[project][self.type_speed]

        if speed_project and total_meters and total_houses:
            slope = speed_project / 7 * total_houses / total_meters
        elif not speed_project and start_project and end_project and total_houses:
            slope = total_houses / (pd.to_datetime(end_project) - pd.to_datetime(start_project)).days * 7
        else:
            slope = None

        if start_project and slope and total_houses:
            n_days = total_houses / slope
            n_days_int = math.floor(n_days)
            domain = DateDomainRange(begin=start_project, n_days=n_days_int - 1)
            # small correction so that the predicted amount == total amount on the last day
            slope_corrected = slope + (n_days - n_days_int) * slope / n_days_int
            line = TimeseriesLine(data=slope_corrected,
                                  domain=domain,
                                  name=self.indicator_name,
                                  max_value=total_houses,
                                  project=project)
        else:
            line = None

        return line
