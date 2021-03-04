import copy

import pandas as pd
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Capacity_analysis.Domain import DateDomainRange, DateDomain
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList
from datetime import timedelta
from toggles import ReleaseToggles

toggles = ReleaseToggles('toggles.yaml')


# TODO: Documentation by Casper van Houten
# TODO: Remove commented code
class PhaseCapacity:

    def __init__(self, df: pd.DataFrame, phase_data: dict, client: str,
                 project: str, holiday_periods: list, poc_ideal_rate_line_masterphase=None, masterphase_data=None):
        """This class enables to calculate and make records of all lines of a phase required for the capacity algorithm
        for a given project.

        Args:
            df (pd.DataFrame): contains the complete set of historical data within a project that is relevant
                               for the capacity algorithm.
            phase_data (dict): contains attributes of the phase such as its start date.
            client (str): specifies to which client the phase belongs.
            project (str): specifies to which project the phase belongs.
            holiday_periods (list): specifies the holiday periods that apply to the project.
            poc_ideal_rate_line_masterphase (object): is an object of the poc ideal rate line of the phase that
                                                      determines the work_stock for this phase. Defaults to None.
            masterphase_data ([type], optional): contains attributes of the phase that controls the work_stock
                                                 for this phase. Defaults to None.
        """
        self.df = df
        self.phase_data = phase_data
        self.client = client
        self.project = project
        self.holiday_periods = holiday_periods
        self.poc_ideal_rate_line_masterphase = poc_ideal_rate_line_masterphase
        self.masterphase_data = masterphase_data
        self.record_list = RecordList()

    def algorithm(self):
        """
        This functions calculates the lines required for this phase and joins them in a record list.
        The logic that is applied per line is specified in the line specific function.
        Returns:
            PhaseCapacity (object): for method chaining
        """

        lines = []
        lines.append(self.calculate_target_rate_line())
        lines.append(self.calculate_poc_real_rate_line())
        lines.append(self.calculate_poc_ideal_rate_line())
        lines.append(self.calculate_poc_verwacht_rate_line())
        if self.poc_ideal_rate_line_masterphase:
            lines.append(self.calculate_work_stock_rate_line())
            lines.append(self.calculate_work_stock_amount_line())

        [self.line_to_record(line) for line in lines]

        return self

    def calculate_target_rate_line(self):
        """This functions calculates the target line expressed in rate per day. The line is based on the start date,
        number of days and performance norm as specified at phase data.

        Returns:
            target rate line (object)
        """
        intercept = self.phase_data['performance_norm_unit']
        domain = DateDomainRange(begin=self.phase_data['start_date'],
                                 n_days=self.phase_data['n_days'])
        line = TimeseriesLine(data=intercept,
                              domain=domain,
                              name='target_indicator',
                              max_value=self.phase_data['total_units'])
        return line

    def calculate_poc_real_rate_line(self):
        """This function calculates the percentage of completion (poc) line given what has been realised so far. This line is
        is expressed in rate per day. The line is based on the historical data of this phase in the given project.

        Returns:
            poc real rate line (object)
        """
        ds = self.df[self.phase_data['phase_column']]
        line = TimeseriesLine(data=ds,
                              name='poc_real_indicator',
                              max_value=self.phase_data['total_units'])
        return line

    def calculate_poc_ideal_rate_line(self):
        """This function calculates the percentage of completion (poc) line given what has been realised so far and what still
        needs to be done to make the target deadline. This line is expressed in rate per day. The line is based on the
        poc real rate line and is extended with the daily rate that is required to make the target deadline.
        In the calculation of the required daily rate also holiday periods with zero activity are taken into account.

        Returns:
            poc ideal rate line (object)
        """
        poc_real_rate_line = self.calculate_poc_real_rate_line()
        target_rate_line = self.calculate_target_rate_line()
        distance_to_max_value = poc_real_rate_line.distance_to_max_value()
        daysleft = poc_real_rate_line.daysleft(end=target_rate_line.domain.end)
        # normal case: when there is still work to do and there is time left before the target deadline
        if (distance_to_max_value > 0) & (daysleft > 0):
            domain = DateDomain(begin=poc_real_rate_line.domain.end, end=target_rate_line.domain.end)
            holidays_in_date_range = self.count_holidays_in_date_range(self.holiday_periods,
                                                                       domain.domain)
            domain = DateDomain(begin=poc_real_rate_line.domain.end,
                                end=target_rate_line.domain.end - timedelta(holidays_in_date_range))
            slope = distance_to_max_value / (daysleft - holidays_in_date_range)

            line = poc_real_rate_line.append(TimeseriesLine(data=slope,
                                                            domain=domain),
                                             skip=1)
        # exception: when there is still work to do but the target deadline has already passed
        elif (distance_to_max_value > 0) & (daysleft <= 0):
            slope = distance_to_max_value / 7  # past deadline, production needs to be finish within a week
            domain = DateDomain(begin=poc_real_rate_line.domain.end, end=pd.Timestamp.now() + timedelta(7))
            line = poc_real_rate_line.append(TimeseriesLine(data=slope,
                                                            domain=domain),
                                             skip=1)
        # no more work to do, so ideal line == realised line
        else:
            line = poc_real_rate_line
        holiday_periods = self.slice_holiday_periods(holiday_periods=self.holiday_periods,
                                                     periods_to_remove=poc_real_rate_line.domain.domain)
        line = self.add_holiday_periods_to_line(line, holiday_periods)
        line.name = 'poc_ideal_indicator'
        line.max_value = self.phase_data['total_units']
        return line

    def calculate_poc_verwacht_rate_line(self):
        """This function calculates the percentage of completion (poc) line given what has been realised so far and what is
        expected that will be done given past performance. This line is expressed in rate per day. The line is based on
        the poc real rate line and is extended with a daily rate that is based on the average performance during
        the last months. In the calculation of the expected daily rate also holiday periods with zero activity are
        taken into account.

        Returns:
            poc real rate line (object)
        """
        poc_real_rate_line = self.calculate_poc_real_rate_line()
        slope = poc_real_rate_line.integrate().extrapolate(data_partition=0.5).slope
        # when there not enough realised data pionts, we take the ideal speed as slope
        if slope == 0:
            slope = self.phase_data['performance_norm_unit']
        distance_to_max_value = poc_real_rate_line.distance_to_max_value()
        daysleft = poc_real_rate_line.daysleft(slope=slope)
        # if there is work to do we extend the pocreal line, if not ideal line == realised line
        if distance_to_max_value > 0:
            domain = DateDomainRange(begin=poc_real_rate_line.domain.end, n_days=daysleft)
            line = poc_real_rate_line.append(TimeseriesLine(data=slope, domain=domain), skip=1)
        else:
            line = poc_real_rate_line
        holiday_periods = self.slice_holiday_periods(self.holiday_periods, poc_real_rate_line.domain.domain)
        line = self.add_holiday_periods_to_line(line, holiday_periods)
        line.name = 'poc_verwacht_indicator'
        line.max_value = self.phase_data['total_units']
        return line

    def calculate_work_stock_rate_line(self):
        """This function calculates the work stock line given the poc ideal rate line of the master phase that controls the
        work_stock for this phase. The work stock line is expressed in rate per day.

        Raises:
            ValueError: this function cannot be executed without the poc ideal rate line of the masterphase.

        Returns:
            work_stock rate line (object)
        """
        if not self.poc_ideal_rate_line_masterphase:
            raise ValueError
        ratio = self.phase_data['total_units'] / self.masterphase_data['total_units']
        line = self.poc_ideal_rate_line_masterphase * ratio
        line.name = 'work_stock_indicator'
        line.max_value = self.phase_data['total_units']
        return line

    def calculate_work_stock_amount_line(self):
        """This function calculates the work stock amount line which specifies the total amount of work stock at
        a given day. This line is expressed in amount per day, not in rates per day. The work stock amount line is
        calculated by subtracting the integral of the poc ideal rate line from the integral of
        the work_stock rate line.

        Returns:
            work stock amount line (object)
        """
        line = self.calculate_work_stock_rate_line().integrate() - self.calculate_poc_ideal_rate_line().integrate()
        line.name = 'work_stock_amount_indicator'
        line.max_value = self.phase_data['total_units']
        return line

    def line_to_record(self, line: object):
        """This functions takes a line object and adds it as a record to the record list.

        Args:
            line (object)
        """
        if toggles.transform_line_record:
            if line.name == 'work_stock_amount_indicator':
                self.record_list.append(LineRecord(record=line,
                                                   collection='Lines',
                                                   graph_name=f'{line.name}',
                                                   phase=self.phase_data['name'],
                                                   client=self.client,
                                                   project=self.project,
                                                   resample_method='mean'))
            else:
                self.record_list.append(LineRecord(record=line,
                                                   collection='Lines',
                                                   graph_name=f'{line.name}',
                                                   phase=self.phase_data['name'],
                                                   client=self.client,
                                                   project=self.project))
        else:
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
