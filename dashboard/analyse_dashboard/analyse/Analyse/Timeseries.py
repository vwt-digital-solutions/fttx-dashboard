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
        self.prognoses_set = False
        self._prognoses()

    def set_timeseries_collection(self):
        self.timeseries_collection = {}
        for project, project_df in self.df.groupby(by="project"):
            self.timeseries_collection[project] = Timeseries(project_df,
                                                             self.column,
                                                             project,
                                                             self.cutoff,
                                                             self.ftu_dates['date_FTU0'][project],
                                                             self.ftu_dates['date_FTU1'][project]
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
            if timeseries.calculate_cumsum_lines:
                timeseries.calculate_cumsum_for_prognoses()
            if timeseries.do_calculate_cumsum_lines_fast():
                slope, _ = linear_regression(timeseries.realised_cumsum_fast)
                total_slope_fast += slope
                # total_intersect_fast += intersect
                counter_fast += 1
            if timeseries.do_calculate_cumsum_lines_slow():
                slope, _ = linear_regression(timeseries.realised_cumsum_slow)
                total_slope_slow += slope
                # total_intersect_slow += intersect
                counter_slow += 1

        if counter_fast > 0:
            self.avg_slope_fast = total_slope_fast / counter_fast
            self.avg_intersect_fast = total_intersect_fast / counter_fast
            print(f'avgs fast: {self.avg_intersect_fast:.2f}+{self.avg_slope_fast:.2f}*x')

        if counter_slow > 0:
            self.avg_slope_slow = total_slope_slow / counter_slow
            self.avg_intersect_slow = total_intersect_slow / counter_slow
            print(f'avgs slow: {self.avg_intersect_slow:.2f}+{self.avg_slope_slow:.2f}*x')

    def _prognoses(self):
        # FIrst we need to calculate avgs over the collection,
        # as we'll need it when a timeseries has insufficient data to calculate its own
        self._calculate_avgs()

        # On second go-round, we can do the prognoses for all
        for project, timeseries in self.timeseries_collection.items():
            timeseries.prognoses(self.avg_slope_fast,
                                 self.avg_intersect_fast,
                                 self.avg_slope_slow,
                                 self.avg_intersect_slow)
        self.prognoses_set = True

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
    def __init__(self, df, column, project, cutoff, ftu_0, ftu_1):
        self.df = df
        self.column = column
        # Should projectname be attr of class?
        self.project = project
        self.cutoff = cutoff
        self.ftu_0 = np.datetime64(ftu_0)
        self.ftu_1 = np.datetime64(ftu_1)
        self.serialize()
        self.calculate_cumsum()
        self.calculate_cumsum_percentage()
        self.get_realised_date_range()
        self.set_target_line()
        # We might not be able to set time shift at init time, or we might not need it at all

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
        try:
            real_dates = self.df[~self.df[self.column].isna()][self.column]
            if len(real_dates) < 2:
                self.calculate_cumsum_lines = False
                self.start_date = self.ftu_0
            else:
                self.start_date = real_dates.min()
                self.calculate_cumsum_lines = True
                start_date_realised = real_dates.min()
                end_date_realised = real_dates.max()
                self.realised_date_range = pd.date_range(start=start_date_realised, end=end_date_realised)
                self.set_realised_data()
        except ValueError:
            raise ValueError(f"start and end can not be 0: {start_date_realised} - {end_date_realised}")

    def do_calculate_cumsum_lines_fast(self):
        do_calculate = False
        if self.calculate_cumsum_lines:
            if self.prognoses_fast:
                do_calculate = True
        return do_calculate

    def do_calculate_cumsum_lines_slow(self):
        do_calculate = False
        if self.calculate_cumsum_lines:
            if self.prognoses_slow:
                do_calculate = True
        return do_calculate

    def set_index(self):
        self.prognoses_date_range = pd.date_range(start='01-01-2019',
                                                  end='31-12-2021',
                                                  freq='D')
        self.timeseries = pd.DataFrame(index=self.prognoses_date_range,
                                       columns=['Aantal'],
                                       data=self.timeseries
                                       ).fillna(0)
        self.timeseries['day_count'] = range(0, len(self.timeseries))

    def clean_timeseries_for_prognoses(self):
        # Remove houses with unrealistic future delivery dates, set them to 0
        self.timeseries.loc[self.timeseries.index > pd.Timestamp.now(), 'Aantal'] = 0

    def calculate_cumsum_for_prognoses(self):
        if self.timeseries['Aantal'].sum() == 0:
            raise ValueError("No prognoses can be done")
        self.realised_cumsum_percentage = self.cumsum_percentage.loc[self.realised_date_range]
        self.realised_cumsum_fast = self.realised_cumsum_percentage[self.realised_cumsum_percentage.Aantal < self.cutoff]
        self.realised_cumsum_slow = self.realised_cumsum_percentage[self.realised_cumsum_percentage.Aantal >= self.cutoff]
        self.prognoses_fast = len(self.realised_cumsum_fast) > 1
        self.prognoses_slow = len(self.realised_cumsum_slow) > 1

    def get_range(self):
        return np.array(list(range(0, len(self.timeseries))))

    def prognoses(self, slope_fast, intersect_fast, slope_slow, intersect_slow):
        if self.do_calculate_cumsum_lines_fast():
            slope_fast_calc, _ = linear_regression(self.realised_cumsum_fast)
            self.slope_fast = slope_fast_calc
            # self.intersect_fast = intersect_fast_calc
        else:
            self.slope_fast = slope_fast
            # self.intersect_fast = intersect_fast

        if self.do_calculate_cumsum_lines_slow():
            slope_slow_calc, _ = linear_regression(self.realised_cumsum_slow)
            self.slope_slow = slope_slow_calc
            # self.intersect_slow = intersect_slow_calc
        else:
            self.slope_slow = slope_slow
            # self.intersect_slow = intersect_slow

        self.intersect_fast = - len(self.prognoses_date_range[self.prognoses_date_range < self.start_date]) * self.slope_fast

        self.prognoses_fast = self.slope_fast * self.get_range() + self.intersect_fast

        index_cutoff = sum(self.prognoses_fast < self.cutoff)
        self.intersect_slow = self.get_intersect_slow(self.prognoses_fast, self.slope_slow, index_cutoff)

        self.prognoses_slow = self.slope_slow * self.get_range() + self.intersect_slow
        self.prognose = np.append(self.prognoses_fast[:index_cutoff], self.prognoses_slow[index_cutoff:])
        self.prognose = self.round_edge_values(self.prognose)
        self.set_prognoses_frame()

    def round_edge_values(self, line):
        line[line > 100] = 100
        line[line < 0] = 0
        return line

    def get_intersect_slow(self, prognoses_fast, slope_slow, index_cutoff):
        if index_cutoff == len(prognoses_fast):
            index_cutoff = index_cutoff - 1
        origin_slow = slope_slow * self.get_range()
        intersect_slow = prognoses_fast[index_cutoff] - origin_slow[index_cutoff]
        return intersect_slow

    def percentage_to_amount(self, percentages):
        return len(self.df) * (percentages / 100)

    def set_realised_data(self):
        self.realised_frame = pd.DataFrame(index=self.realised_date_range)
        self.realised_frame['cumsum_percentage'] = self.cumsum_percentage.loc[self.realised_date_range].Aantal
        self.realised_frame['cumsum_amount'] = self.cumsum_series.loc[self.realised_date_range].Aantal

    def set_target_line(self):
        offset = np.timedelta64(14, 'D')
        date_range = np.arange(self.ftu_0 + offset, self.ftu_1 - offset)
        self.target_frame = pd.DataFrame(index=date_range)
        x_range = np.array(range(0, len(date_range)))
        self.target_per_day = 100 / (len(date_range) - 28)
        self.target_frame['y_target_percentage'] = self.target_per_day * x_range
        self.target_frame['y_target_amount'] = self.percentage_to_amount(self.target_frame['y_target_percentage'])

    def set_prognoses_frame(self):
        self.prognoses_frame = pd.DataFrame(index=self.prognoses_date_range)
        self.prognoses_frame['prognose_percentage'] = self.prognose
        self.prognoses_frame['prognose_amount'] = self.percentage_to_amount(self.prognoses_frame['prognose_percentage'])

    def get_prognoses_frame(self):
        return self.prognoses_frame

    def get_realised_frame(self):
        try:
            realised_frame = self.realised_frame
        except AttributeError:
            realised_frame = pd.DataFrame()
        return realised_frame

    def get_target_frame(self):
        return self.target_frame

    def get_timeseries_frame(self):
        prognose = self.get_prognoses_frame()
        target = self.get_target_frame()
        realised = self.get_realised_frame()
        self.complete_frame = pd.merge(prognose, target, how='left', left_index=True, right_index=True)
        self.complete_frame = pd.merge(self.complete_frame, realised, how='left', left_index=True, right_index=True)
        return self.complete_frame
