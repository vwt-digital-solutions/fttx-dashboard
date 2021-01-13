import pandas as pd

from Analyse.Capacity_analysis.Line import TimeseriesLine


class PhaseCapacity:

    def __init__(self, df: pd.DataFrame):
        self.production_by_day = TimeseriesLine(df.groupby([df.index]).count())
        self.capacity_by_day = None
        self.target_by_day = None

    def algorithm(self):
        self.production_over_time = self.production_by_day.integrate()
        return self

    def get_record(self):
        return self.__dict__

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
