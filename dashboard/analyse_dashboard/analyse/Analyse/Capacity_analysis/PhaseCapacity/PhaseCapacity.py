import pandas as pd

from Analyse.Capacity_analysis.Line import TimeseriesLine, LinearLine
from Analyse.Capacity_analysis.Domain import DateDomainRange


class PhaseCapacity:
    """
        :param df: One-column dataframe, should have a datetime-index
    """

    def __init__(self, df: pd.DataFrame, phases_config: dict, phases_projectspecific: dict):
        self.production_by_day = TimeseriesLine(df.groupby([df.index]).count())
        self.capacity_by_day = None
        self.target_by_day = None
        self.client = None
        self.record_dict = RecordListWrapper(client=self.client)
        self.phase = None
        self.phases_config = phases_config
        self.phases_projectspecific = phases_projectspecific

    def algorithm(self):
        """
        Algorithm to be ran, will contain all logic related to capacity Lines per Phase.
        :return: PhaseCapacity object, used for Method chaining.
        """
        production_over_time = self.production_by_day.integrate()
        self._to_record(production_by_day=self.production_by_day,
                        production_over_time=production_over_time,
                        )
        self.capacity_by_day_indicator()
        self.production_over_time = self.production_by_day.integrate()
        self.target_over_time = LinearLine(slope=self.phases_projectspecific['performance_norm_unit'],
                                           intercept=0,
                                           domain=DateDomainRange(begin=self.phases_projectspecific['start_date'],
                                           n_days=self.phases_config['n_days']))
        return self

    def capacity_by_day_indicator(self):
        """
        Example Function
        :return:
        """
        capacity_by_day_indicator = self.capacity_by_day.integrate()
        return capacity_by_day_indicator

    def _to_record(self, **kwargs):
        self.record = Record(kwargs)

    # def get_record(self):
    #     record_dict.add(self.capacity_by_day_indicator(), LineRecord, phase=self.phase)
    #     return record_dict

    # def get_project_lines(self, fase_delta, project_df, geulen_line=None):
    #     print('getting production per day')
    #     production_by_day = TimeseriesLine(project_df.groupby([project_df.index]).count())
    #     print('getting production over time')
    #     production_over_time = production_by_day.integrate()
    #     production_over_time.set_name('production_over_time')
    #     print('getting extrapolation')
    #     extrapolated_line = production_over_time.extrapolate()
    #     extrapolated_line.set_name('extrapolated_line')
    #     if geulen_line:
    #         forecast_line = geulen_line.translate_x(fase_delta)
    #         forecast_line.set_name('forecast_line')
    #     else:
    #         forecast_line = None
    #     required_production = None  # get_required_production(production_over_time, target_line)
    #     print('Combining and making graph')
    #     lines = [line for line in [production_over_time, extrapolated_line, forecast_line, required_production]
    #              if line is not None]
    #     return ProjectGraph(lines=lines)
