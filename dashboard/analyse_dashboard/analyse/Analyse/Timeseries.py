from functions import linear_regression
import numpy as np
import pandas as pd


class Timeseries_collection():
    def __init__(self, df, column, cutoff, ftu_dates):
        self.df = df
        self.column = column
        self.cutoff = cutoff
        self.ftu_dates = ftu_dates
        self.set_timeseries_collection()
        self.extrapolation_set = False
        self._set_extrapolation()

    def set_timeseries_collection(self):
        self.timeseries_collection = {}
        for project, project_df in self.df.groupby(by="project"):
            self.timeseries_collection[project] = Timeseries(project_df,
                                                             self.column,
                                                             project,
                                                             self.cutoff,
                                                             self.ftu_dates['date_FTU0'][project],
                                                             self.ftu_dates['date_FTU1'][project],
                                                             civil_startdate=pd.to_datetime('2020-01-01'),
                                                             fase_delta=0,
                                                             bis_slope=1
                                                             )

    def set_min_date(self):
        # Does this yield intended result?
        date_col = self.df[~self.df[self.column].isna()][self.column]
        self.min_date = min(date_col)

    def _calculate_avgs(self):
        # Calculates the average slopes and intersections of the fit for each timeseries in the collection
        total_slope_fast = 0
        total_intersect_fast = 0
        total_slope_slow = 0
        total_intersect_slow = 0
        counter_fast = 0
        counter_slow = 0
        for project, timeseries in self.timeseries_collection.items():
            if timeseries.calculate_extrapolation:
                timeseries.calculate_cumsum_for_extrapolation()
            if timeseries.do_calculate_extrapolation_fast():
                slope, _ = linear_regression(timeseries.realised_cumsum_fast)
                total_slope_fast += slope
                # total_intersect_fast += intersect
                counter_fast += 1
            if timeseries.do_calculate_extrapolation_slow():
                slope, _ = linear_regression(timeseries.realised_cumsum_slow)
                total_slope_slow += slope
                # total_intersect_slow += intersect
                counter_slow += 1

        if counter_fast > 0:
            self.avg_slope_fast = total_slope_fast / counter_fast
            self.avg_intersect_fast = total_intersect_fast / counter_fast

        if counter_slow > 0:
            self.avg_slope_slow = total_slope_slow / counter_slow
            self.avg_intersect_slow = total_intersect_slow / counter_slow
        else:
            self.avg_slope_slow = self.avg_slope_fast
            self.avg_intersect_slow = self.avg_intersect_fast

    def _set_extrapolation(self):
        # FIrst we need to calculate avgs over the collection,
        # as we'll need it when a timeseries has insufficient data to calculate its own
        self._calculate_avgs()

        # On second go-round, we can do the extrapolation for all
        for project, timeseries in self.timeseries_collection.items():
            timeseries.set_extrapolation(self.avg_slope_fast,
                                         self.avg_slope_slow,
                                         )
        self.extrapolation_set = True

    def get_timeseries_frame(self):
        timeseries_dict = {}
        for project, timeseries in self.timeseries_collection.items():
            timeseries_dict[project] = timeseries.get_timeseries_frame()
        complete_frame = pd.concat(timeseries_dict, axis=1)

        # We'll calculate the totals over the projects here as well.
        idx = pd.IndexSlice
        for col in complete_frame.columns.get_level_values(1).unique():
            if 'amount' in col:
                complete_frame['Totaal', col] = complete_frame.loc[idx[:], idx[:, col]].sum(axis=1)

        return complete_frame


class Timeseries():
    def __init__(self, df, column, project, cutoff, ftu_0, ftu_1, civil_startdate,
                 fase_delta, bis_slope, slope_geulen=0, intersect_geulen=0, start_date_geulen=0):
        self.df = df
        self.column = column
        # Should projectname be attr of class?
        self.project = project
        self.cutoff = cutoff
        self.ftu_0 = np.datetime64(ftu_0)
        self.ftu_1 = np.datetime64(ftu_1)
        self.civil_startdate = civil_startdate
        self.slope_geulen = slope_geulen
        self.start_date_geulen = start_date_geulen
        self.intersect_geulen = intersect_geulen
        self.fase_delta = fase_delta
        self.bis_slope = bis_slope
        self.serialize()
        self.calculate_cumsum()
        self.calculate_cumsum_percentage()
        self.get_realised_date_range()
        self.get_extrapolation_date_range()
        self.set_realised_phase()
        self.calculate_cumsum_for_extrapolation()
        self.set_target_frame()
        self.set_target_phase(self.bis_slope, self.fase_delta)
        self.set_extrapolation_phase()
        self.set_forecast_phase(self.start_date_geulen, self.slope_geulen, self.intersect_geulen, self.fase_delta)

    def serialize(self):
        self.timeseries = self.df.groupby([self.column]).agg({'sleutel': 'count'}).rename(columns={'sleutel': 'Aantal'})
        self.set_index()

    def calculate_cumsum(self):
        self.cumsum_series = self.timeseries['Aantal'].cumsum().to_frame()
        self.cumsum_series['day_count'] = self.timeseries.day_count

    def calculate_cumsum_percentage(self):
        self.cumsum_percentage = (self.cumsum_series['Aantal'] / len(self.df) * 100).to_frame()
        self.cumsum_percentage['day_count'] = self.timeseries.day_count

    def get_realised_date_range(self):
        self.real_dates = self.df[~self.df[self.column].isna()][self.column]
        if not self.real_dates.empty:
            start_date_realised = self.real_dates.min()
            end_date_realised = self.real_dates.max()
            self.realised_date_range = pd.date_range(start=start_date_realised, end=end_date_realised)

    def get_extrapolation_date_range(self):
        if len(self.real_dates) < 2:
            self.calculate_extrapolation = False
            self.start_date = self.ftu_0
        else:
            self.calculate_extrapolation = True
            self.start_date = self.real_dates.min()
            start_date_realised = self.real_dates.min()
            end_date_realised = self.real_dates.max()
            self.extrapolation_date_range = pd.date_range(start=start_date_realised, end=end_date_realised)

    def do_calculate_extrapolation_fast(self):
        do_calculate = False
        if self.calculate_extrapolation:
            if self.extrapolation_fast:
                do_calculate = True
        return do_calculate

    def do_calculate_extrapolation_slow(self):
        do_calculate = False
        if self.calculate_extrapolation:
            if self.extrapolation_slow:
                do_calculate = True
        return do_calculate

    def set_index(self):
        self.timeseries_date_range = pd.date_range(start='01-01-2019',
                                                   end='31-12-2021',
                                                   freq='D')
        self.timeseries = pd.DataFrame(index=self.timeseries_date_range,
                                       columns=['Aantal'],
                                       data=self.timeseries
                                       ).fillna(0)
        self.timeseries['day_count'] = range(0, len(self.timeseries))

    def clean_timeseries_for_extrapolation(self):
        # Remove houses with unrealistic future delivery dates, set them to 0
        self.timeseries.loc[self.timeseries.index > pd.Timestamp.now(), 'Aantal'] = 0

    def calculate_cumsum_for_extrapolation(self):
        if self.calculate_extrapolation:
            self.realised_cumsum_percentage = self.cumsum_percentage.loc[self.extrapolation_date_range]
            self.realised_cumsum_fast = self.realised_cumsum_percentage[self.realised_cumsum_percentage.Aantal < self.cutoff]
            self.realised_cumsum_slow = self.realised_cumsum_percentage[self.realised_cumsum_percentage.Aantal >= self.cutoff]
            self.extrapolation_fast = len(self.realised_cumsum_fast) > 1
            self.extrapolation_slow = len(self.realised_cumsum_slow) > 1

    def get_range(self):
        return np.array(list(range(0, len(self.timeseries))))

    def slopes_splitwise_linear_regression(self, slope_fast, slope_slow):
        if self.do_calculate_extrapolation_fast():
            slope_fast, intersect = linear_regression(self.realised_cumsum_fast)
        else:
            intersect = 0

        if self.do_calculate_extrapolation_slow():
            slope_slow, _ = linear_regression(self.realised_cumsum_slow)

        return slope_fast, intersect, slope_slow

    def set_extrapolation(self, slope_fast, slope_slow):
        slope_fast, intersect, slope_slow = self.slopes_splitwise_linear_regression(slope_fast, slope_slow)
        line = self.make_linear_line(slope_fast, self.start_date, intersect=intersect)
        line = self.add_second_line(line, slope_slow)
        self.extrapolation = self.round_edge_values(line)
        self.set_extrapolation_frame()

    def slope_linear_regression(self):
        if self.do_calculate_extrapolation_fast():
            slope, intersect = linear_regression(self.realised_cumsum_fast)
        return slope, intersect

    def make_linear_line(self, slope, start_date, fase_delta=0, intersect=0):
        line = slope * self.get_range() + intersect - fase_delta * slope
        return line

    def add_second_line(self, line, slope_slow):
        index_cutoff = sum(line < self.cutoff)
        intersect_2 = self.get_intersect_2(line, slope_slow, index_cutoff)
        line_2 = slope_slow * self.get_range() + intersect_2
        line = np.append(line[:index_cutoff], line_2[index_cutoff:])
        return line

    def round_edge_values(self, line):
        line[line > 100] = 100
        line[line < 0] = 0
        return line

    def get_intersect_2(self, extrapolation_fast, slope_slow, index_cutoff):
        if index_cutoff == len(extrapolation_fast):
            index_cutoff = index_cutoff - 1
        origin_slow = slope_slow * self.get_range()
        intersect_slow = extrapolation_fast[index_cutoff] - origin_slow[index_cutoff]
        return intersect_slow

    def percentage_to_amount(self, percentages):
        return len(self.df) * (percentages / 100)

    def set_realised_phase(self):
        if not self.real_dates.empty:
            self.realised_phase = pd.DataFrame(index=self.realised_date_range)
            self.realised_phase['cumsum_percentage'] = self.cumsum_percentage.loc[self.realised_date_range].Aantal
            self.realised_phase['cumsum_amount'] = self.cumsum_series.loc[self.realised_date_range].Aantal

    def set_target_frame(self):
        offset = np.timedelta64(14, 'D')
        start_date = self.ftu_0 + offset
        slope = 100 / (len(np.arange(self.ftu_0 + offset, self.ftu_1 - offset)))
        line = self.make_linear_line(slope, start_date)
        self.target = self.round_edge_values(line)

        self.target_frame = pd.DataFrame(index=self.timeseries_date_range)
        self.target_frame['y_target_percentage'] = self.target
        self.target_frame['y_target_amount'] = self.percentage_to_amount(self.target_frame['y_target_percentage'])

    def set_target_phase(self, slope, fase_delta):
        start_date = self.civil_startdate
        line = self.make_linear_line(slope, start_date, fase_delta)
        self.target_line = self.round_edge_values(line)

        self.target_phase = pd.DataFrame(index=self.timeseries_date_range)
        self.target_phase['y_target_percentage'] = self.target_line
        self.target_phase['y_target_amount'] = self.percentage_to_amount(self.target_phase['y_target_percentage'])

    def set_extrapolation_phase(self):
        if self.calculate_extrapolation:
            start_date = self.start_date
            self.slope, self.intersect = self.slope_linear_regression()
            line = self.make_linear_line(self.slope, start_date, intersect=self.intersect)
            self.extrapolation_line = self.round_edge_values(line)

            self.extrapolation_phase = pd.DataFrame(index=self.timeseries_date_range)
            self.extrapolation_phase['extrapolation_percentage'] = self.extrapolation_line
            self.extrapolation_phase['extrapolation_amount'] = self.percentage_to_amount(
                self.extrapolation_phase['extrapolation_percentage'])

    def set_extrapolation_frame(self):
        self.extrapolation_frame = pd.DataFrame(index=self.timeseries_date_range)
        self.extrapolation_frame['extrapolation_percentage'] = self.extrapolation
        self.extrapolation_frame['extrapolation_amount'] = self.percentage_to_amount(self.extrapolation_frame['extrapolation_percentage'])

    def set_forecast_phase(self, start_date_geulen, slope_geulen, intersect_geulen, fase_delta):
        if slope_geulen > 0:
            line = self.make_linear_line(slope_geulen, start_date_geulen, fase_delta, intersect=intersect_geulen)
            self.forecast_line = self.round_edge_values(line)
            self.forecast_phase = pd.DataFrame(index=self.timeseries_date_range)
            self.forecast_phase['forecast_percentage'] = self.forecast_line
            self.forecast_phase['forecast_amount'] = self.percentage_to_amount(self.forecast_phase['forecast_percentage'])

    def get_extrapolation_frame(self):
        return self.extrapolation_frame

    def get_target_frame(self):
        return self.target_frame

    def get_target_phase(self):
        return self.target_phase

    def get_realised_phase(self):
        try:
            realised_phase = self.realised_phase
        except AttributeError:
            realised_phase = pd.DataFrame()
        return realised_phase

    def get_extrapolation_phase(self):
        if not self.calculate_extrapolation:
            self.extrapolation_phase = pd.DataFrame()
        return self.extrapolation_phase

    def get_forecast_phase(self):
        return self.forecast_phase

    def get_timeseries_frame(self):
        extrapolation = self.get_extrapolation_frame()
        target = self.get_target_frame()
        extrapolation_phase = self.get_extrapolation_phase()
        target_phase = self.get_target_phase()
        realised_phase = self.get_realised_phase()
        forecast_phase = self.get_forecast_phase()
        self.complete_frame = pd.merge(extrapolation, target, how='left', left_index=True, right_index=True)
        self.complete_frame = pd.merge(self.complete_frame, realised_phase, how='left', left_index=True, right_index=True)
        self.complete_frame = pd.merge(self.complete_frame, target_phase, how='left', left_index=True, right_index=True)
        self.complete_frame = pd.merge(self.complete_frame, extrapolation_phase, how='left', left_index=True, right_index=True)
        self.complete_frame = pd.merge(self.complete_frame, forecast_phase, how='left', left_index=True, right_index=True)
        return self.complete_frame
