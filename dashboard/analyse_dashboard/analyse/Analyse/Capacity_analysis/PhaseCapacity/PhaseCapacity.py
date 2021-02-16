import copy

import pandas as pd
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Capacity_analysis.Domain import DateDomainRange, DateDomain
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList
from datetime import timedelta


# TODO: Documentation by Casper van Houten
# TODO: Remove commented code
class PhaseCapacity:
    """

    Args:
        df (pd.DataFrame): One-column dataframe, should have a datetime-index
        phases_config:
    """

    def __init__(self, df: pd.DataFrame, phase_data: dict, client: str,
                 project: str, holiday_periods: list, pocideal_line_masterphase=None, masterphase_data=None):
        self.df = df
        self.phase_data = phase_data
        self.client = client
        self.project = project
        self.holiday_periods = holiday_periods
        self.pocideal_line_masterphase = pocideal_line_masterphase
        self.masterphase_data = masterphase_data
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

        lines = []
        lines.append(self.calculate_target_line())
        lines.append(self.calculate_pocreal_line())
        lines.append(self.calculate_pocideal_line())
        lines.append(self.calculate_pocverwacht_line())
        if self.pocideal_line_masterphase:
            lines.append(self.calculate_werkvoorraad_line())
            lines.append(self.calculate_werkvoorraadabsoluut_line())

        [self.line_to_record(line) for line in lines]

        return self

    def calculate_target_line(self):
        intercept = self.phase_data['performance_norm_unit']
        domain = DateDomainRange(begin=self.phase_data['start_date'],
                                 n_days=self.phase_data['n_days'])
        line = TimeseriesLine(data=intercept,
                              domain=domain,
                              name='target_indicator',
                              max_value=self.phase_data['total_units'])
        return line

    def calculate_pocreal_line(self):
        ds = self.df[self.phase_data['phase_column']]
        line = TimeseriesLine(data=ds,
                              name='poc_real_indicator',
                              max_value=self.phase_data['total_units'])
        return line

    def calculate_pocideal_line(self):
        pocreal_line = self.calculate_pocreal_line()
        target_line = self.calculate_target_line()
        distance_to_max_value = pocreal_line.distance_to_max_value()
        daysleft = pocreal_line.daysleft(end=target_line.domain.end)
        # normal case: when there is still work to do and there is time left before the target deadline
        if (distance_to_max_value > 0) & (daysleft > 0):
            domain = DateDomain(begin=pocreal_line.domain.end, end=target_line.domain.end)
            holidays_in_date_range = self.count_holidays_in_date_range(self.holiday_periods,
                                                                       domain.domain)
            domain = DateDomain(begin=pocreal_line.domain.end,
                                end=target_line.domain.end - timedelta(holidays_in_date_range))
            slope = distance_to_max_value / (daysleft - holidays_in_date_range)

            line = pocreal_line.append(TimeseriesLine(data=slope,
                                                      domain=domain),
                                       skip=1)
        # exception: when there is still work to do but the target deadline has already passed
        elif (distance_to_max_value > 0) & (daysleft <= 0):
            slope = distance_to_max_value / 7  # past deadline, production needs to be finish within a week
            domain = DateDomain(begin=pocreal_line.domain.end, end=pd.Timestamp.now() + timedelta(7))
            line = pocreal_line.append(TimeseriesLine(data=slope,
                                                      domain=domain),
                                       skip=1)
        # no more work to do, so ideal line == realised line
        else:
            line = pocreal_line
        holiday_periods = self.slice_holiday_periods(holiday_periods=self.holiday_periods,
                                                     periods_to_remove=pocreal_line.domain.domain)
        line = self.add_holiday_periods_to_line(line, holiday_periods)
        line.name = 'poc_ideal_indicator'
        line.max_value = self.phase_data['total_units']
        return line

    def calculate_pocverwacht_line(self):
        pocreal_line = self.calculate_pocreal_line()
        slope = pocreal_line.integrate().extrapolate(data_partition=0.5).slope
        # when there not enough realised data pionts, we take the ideal speed as slope
        if slope == 0:
            slope = self.phase_data['performance_norm_unit']
        distance_to_max_value = pocreal_line.distance_to_max_value()
        daysleft = pocreal_line.daysleft(slope=slope)
        # if there is work to do we extend the pocreal line, if not ideal line == realised line
        if distance_to_max_value > 0:
            domain = DateDomainRange(begin=pocreal_line.domain.end, n_days=daysleft)
            line = pocreal_line.append(TimeseriesLine(data=slope, domain=domain), skip=1)
        else:
            line = pocreal_line
        holiday_periods = self.slice_holiday_periods(self.holiday_periods, pocreal_line.domain.domain)
        line = self.add_holiday_periods_to_line(line, holiday_periods)
        line.name = 'poc_verwacht_indicator'
        line.max_value = self.phase_data['total_units']
        return line

    def calculate_werkvoorraad_line(self):
        if not self.pocideal_line_masterphase:
            raise ValueError
        ratio = self.phase_data['total_units'] / self.masterphase_data['total_units']
        line = self.pocideal_line_masterphase * ratio
        line.name = 'werkvoorraad_indicator'
        line.max_value = self.phase_data['total_units']
        return line

    def calculate_werkvoorraadabsoluut_line(self):
        line = self.calculate_werkvoorraad_line().integrate() - self.calculate_pocideal_line().integrate()
        line.name = 'werkvoorraad_absoluut_indicator'
        line.max_value = self.phase_data['total_units']
        return line

    def line_to_record(self, line: object):
        self.record_list.append(LineRecord(record=line,
                                           collection='Lines',
                                           graph_name=f'{line.name}',
                                           phase=self.phase_data['name'],
                                           client=self.client,
                                           project=self.project))

    # TODO: Documentation by Casper van Houten
    def get_record(self):
        return self.record_list

    @staticmethod
    def slice_holiday_periods(holiday_periods, periods_to_remove):
        """
        Slice holiday periods to only contain relevant dates. Used to ensure some holidays are not counted doubly.
        Args:
            holiday_periods:
            periods_to_remove:

        Returns: A sliced set of holiday periods

        """
        new_holiday_periods = []

        for base_holiday_period in holiday_periods:
            min_date = base_holiday_period.min()
            max_date = base_holiday_period.max()
            remove_start = min_date in periods_to_remove
            remove_end = max_date in periods_to_remove
            if not remove_start and not remove_end:
                new_holiday_periods.append(base_holiday_period)
            elif remove_start and not remove_end:
                new_holiday_periods.append(
                    pd.date_range(periods_to_remove.max() + timedelta(days=1), base_holiday_period[-1]))
            elif not remove_start and remove_end:
                new_holiday_periods.append(
                    pd.date_range(base_holiday_period[0], periods_to_remove.min() + timedelta(days=-1)))
        return new_holiday_periods

    @staticmethod
    def count_holidays_in_date_range(holidays, date_range):
        """
        Counts the amount of holidays in a given date range
        Args:
            holidays: Set of date ranges which are considered holidays
            date_range: target range in which the holidays are counted

        Returns: The amount of holidays in the date range.
        """
        count = 0
        for holiday in holidays:
            count += len(set(holiday).intersection(set(date_range)))
        return count

    def add_holiday_periods_to_line(self, line, sorted_holiday_periods):
        """
        Function that will enhance a line to include input rest periods.
        The productivity during the rest period will be 0, and the line will be extended to keep the same total.
        Args:
            line:
            sorted_rest_dates:

        Returns:

        """
        holiday_periods = copy.deepcopy(sorted_holiday_periods)  # Retrieve full set of defined rest dates
        # Main loop.
        # You have to loop over the rest periods multiple times,
        # because you are extending the timeperiod in every loop
        while True:
            # Find all relevant rest periods, given current dates of the line
            next_holiday_period, other_periods = self._find_next_holiday_periods_in_date_range(line.make_series().index,
                                                                                               holiday_periods)
            if not len(next_holiday_period):
                break  # Stop looping if there's no rest periods left to add
            # Remove rest periods that have been added from the set that can still be added
            holiday_periods = other_periods
            # Add next relevant rest periods to the line, continue with the new line
            line = self._add_holiday_period(line, next_holiday_period)
        return line

    def _add_holiday_period(self, line, holiday_period):
        """
        Helper function to add a single rest period to a TimeseriesLine
        Args:
            line:
            rest_period:

        Returns:

        """
        holiday_period_line = TimeseriesLine(domain=DateDomain(begin=holiday_period[0], end=holiday_period[-1]), data=0)
        before_line = line.slice(end=holiday_period.min())
        after_line = line.slice(begin=holiday_period.min()).translate_x(len(holiday_period))
        return before_line.append(holiday_period_line, skip=1, skip_base=True).append(after_line)

    # Rest dates have to be sorted to yield correct results!!
    def _find_next_holiday_periods_in_date_range(self, date_range, holidays_period):
        """
        Helper function to find the next rest period in the given set of rest dates.
        Args:
            date_range:
            rest_dates:

        Returns:

        """
        overlapping_dates = []
        while len(holidays_period) > 0:
            dates = holidays_period.pop(0)
            overlapping_dates = self._find_overlapping_dates(date_range, dates)
            if overlapping_dates:
                overlapping_dates = pd.date_range(start=overlapping_dates[0], end=overlapping_dates[-1], freq='D')
                break
        return overlapping_dates, holidays_period

    def _find_overlapping_dates(self, base_period, holidays_period):
        if holidays_period.min() in base_period:
            overlapping_dates = holidays_period.to_list()
        else:
            overlapping_dates = [date for date in holidays_period if date in base_period]
        return overlapping_dates

    def _remove_holiday_periods(self, holidays_period, to_remove):
        new_list = [x for x in holidays_period if not to_remove]
        if len(new_list) == len(holidays_period):
            raise ValueError('Did not remove value from list, this would result in infinite loop')
        return new_list
