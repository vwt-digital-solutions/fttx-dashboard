import pandas as pd

from Analyse.Capacity_analysis.Line import TimeseriesLine, LinearLine
from Analyse.Capacity_analysis.Domain import DateDomainRange, DateDomain
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


# TODO: Documentation by Casper van Houten
# TODO: Remove commented code
class PhaseCapacity:
    """

    Args:
        df (pd.DataFrame): One-column dataframe, should have a datetime-index
        phases_config:
        phases_projectspecific:
    """

    def __init__(self, df: pd.DataFrame, phases_config: dict, phases_projectspecific: dict, phase=None, client=None):
        self.df = df
        self.phase = phase
        self.client = client
        self.phases_config = phases_config
        self.phases_projectspecific = phases_projectspecific
        self.record_list = RecordList()

    def algorithm(self):
        """
        Algorithm to be ran, will contain all logic related to capacity Lines per Phase.
        The following indicators are made:

        - a target line that indicates the number of units per day that need to be produced
          in the specific period over the duration of the project.

        Returns:
             PhaseCapacity: used for Method chaining.

        """
        # calculate target indicator
        self.target_over_time = LinearLine(slope=self.phases_projectspecific['performance_norm_unit'],
                                           intercept=0,
                                           domain=DateDomainRange(begin=self.phases_projectspecific['start_date'],
                                                                  n_days=self.phases_config['n_days']),
                                           name='target_indicator')
        target_over_time_record = LineRecord(record=self.target_over_time,
                                             collection='Lines',
                                             graph_name=f'{self.client}+{self.phase}+{self.target_over_time.name}',
                                             phase=self.phase,
                                             client=self.client)
        # calculate realised production over time
        ds = self.df[(~self.df.isna()) & (self.df <= pd.Timestamp.now())]
        self.production_over_time_realised = TimeseriesLine(ds.groupby(ds).count(),
                                                            domain=DateDomain(begin=ds.index[0],
                                                                              end=ds.index[-1]))
        # # calculate ideal production over time
        # slope = (self.target_over_time.integrate().make_series().max()
        #          - self.production_over_time_realised.integrate().make_series().max()) / \
        #         (self.target_over_time.make_series().index[-1] - self.production_over_time_realised.make_series().index[-1]).days
        # production_over_time_extrapolated_ideal = LinearLine(slope=slope,
        #                                                      intercept=self.production_over_time_realised.integrate().make_series().max(),
        #                                                      domain=DateDomain(begin=str(self.production_over_time_realised.make_series().index[-1]),
        #                                                      end=str(self.target_over_time.make_series().index[-1])))
        # production_over_time_ideal = TimeseriesLine(pocideal_real.make_series().iloc[:-1].add(pocideal_extrap.make_series(),
        #                                             fill_value=0))

        self.record_list.append(target_over_time_record)
        return self

    def capacity_by_day_indicator(self):
        capacity_by_day_indicator = self.capacity_by_day.integrate()
        return capacity_by_day_indicator

    # TODO: Documentation by Casper van Houten
    def get_record(self, **kwargs):
        return self.record_list

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
