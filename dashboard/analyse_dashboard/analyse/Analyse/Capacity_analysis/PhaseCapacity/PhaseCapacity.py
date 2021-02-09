import pandas as pd
from datetime import timedelta
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
    """

    def __init__(self, df: pd.DataFrame, phase_data: dict, client: str,
                 project: str, pocideal_line_masterphase=None, masterphase_data=None):
        self.df = df
        self.phase_data = phase_data
        self.client = client
        self.project = project
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
        line = LinearLine(intercept=intercept,
                          slope=0,
                          domain=domain,
                          name='target_indicator')
        return line

    def calculate_pocreal_line(self):
        ds = self.df[self.phase_data['phase_column']]
        line = TimeseriesLine(data=ds,
                              name='poc_real_indicator')
        return line

    def calculate_pocideal_line(self):
        pocreal_line = self.calculate_pocreal_line()
        target_line = self.calculate_target_line()

        todo = self.phase_data['total_units'] - pocreal_line.integrate().get_most_recent_point()
        daysleft = (target_line.domain.end - pocreal_line.domain.end).days
        if (todo > 0) & (daysleft > 0):
            slope = todo / daysleft
            domain = DateDomain(begin=pocreal_line.domain.end, end=target_line.domain.end)
            line = pocreal_line.append(LinearLine(intercept=slope, slope=0, domain=domain), skip=1)
        if (todo > 0) & (daysleft <= 0):  # production needed to finish within a week
            slope = todo / 7
            domain = DateDomain(begin=pocreal_line.domain.end, end=pd.Timestamp.now() + timedelta(7))
            line = pocreal_line.append(LinearLine(intercept=slope, slope=0, domain=domain), skip=1)
        else:
            line = pocreal_line
        line.name = 'poc_ideal_indicator'
        return line

    def calculate_pocverwacht_line(self):
        pocreal_line = self.calculate_pocreal_line()
        slope = pocreal_line.integrate().extrapolate(data_partition=0.5).slope
        if slope == 0:
            slope = self.phase_data['performance_norm_unit']
        todo = self.phase_data['total_units'] - pocreal_line.integrate().get_most_recent_point()
        daysleft = int(todo / slope)
        if todo > 0:
            domain = DateDomainRange(begin=pocreal_line.domain.end, n_days=daysleft)
            line = pocreal_line.append(LinearLine(intercept=slope, slope=0, domain=domain), skip=1)
        else:
            line = pocreal_line
        line.name = 'poc_verwacht_indicator'
        return line

    def calculate_werkvoorraad_line(self):
        if not self.pocideal_line_masterphase:
            raise ValueError
        ratio = self.phase_data['total_units'] / self.masterphase_data['total_units']
        line = self.pocideal_line_masterphase * ratio
        line.name = 'werkvoorraad_indicator'
        return line

    def calculate_werkvoorraadabsoluut_line(self):
        line = self.calculate_werkvoorraad_line().integrate() - self.calculate_pocideal_line().integrate()
        line.name = 'werkvoorraad_absoluut_indicator'
        return line

    def line_to_record(self, line: object):
        self.record_list.append(LineRecord(record=line,
                                           collection='Lines',
                                           graph_name=f'{line.name}',
                                           phase=self.phase_data['name'],
                                           client=self.client,
                                           project=self.project))

    # TODO: Documentation by Casper van Houten
    def get_record(self, **kwargs):
        return self.record_list

    #
    def add_rest_periods_to_line(self, line, sorted_rest_dates):
        '''
        Function that will enhance a line to include input rest periods.
        The productivity during the rest period will be 0, and the line will be extended to keep the same total.
        Args:
            line:
            sorted_rest_dates:

        Returns:

        '''
        rest_periods = sorted_rest_dates  # Retrieve full set of defined rest dates
        # Main loop.
        # You have to loop over the rest periods multiple times,
        # because you are extending the timeperiod in every loop
        while True:
            # Find all relevant rest periods, given current dates of the line
            next_rest_period, other_periods = self._find_next_rest_periods_in_date_range(line.make_series().index,
                                                                                         rest_periods)
            if not next_rest_period:
                break  # Stop looping if there's no rest periods left to add
            # Remove rest periods that have been added from the set that can still be added
            rest_periods = other_periods
            # Add next relevant rest periods to the line, continue with the new line
            line = self._add_rest_period(line, next_rest_period)
        return line

    def _add_rest_period(self, line, rest_period):
        '''
        Helper function to add a single rest period to a TimeseriesLine
        Args:
            line:
            rest_period:

        Returns:

        '''
        rest_period_line = TimeseriesLine(pd.Series(index=rest_period, data=0))
        before_line = line.slice(end=rest_period.min())
        after_line = line.slice(begin=rest_period.min()).translate_x(len(rest_period))
        return before_line.append(rest_period_line, skip=1, skip_base=True).append(after_line)

    # Rest dates have to be sorted to yield correct results!!
    def _find_next_rest_periods_in_date_range(self, date_range, rest_dates):
        '''
        Helper function to find the next rest period in the given set of rest dates.
        Args:
            date_range:
            rest_dates:

        Returns:

        '''
        overlapping_dates = None
        while len(rest_dates) > 0:
            dates = rest_dates.pop(0)
            overlapping_dates = self._find_overlapping_dates(date_range, dates)
            if overlapping_dates:
                overlapping_dates = pd.date_range(start=overlapping_dates[0], end=overlapping_dates[-1], freq='D')
                break
        return overlapping_dates, rest_dates

    def _find_overlapping_dates(self, base_period, rest_period):
        if rest_period.min() in base_period:
            overlapping_dates = rest_period.to_list()
        else:
            overlapping_dates = [date for date in rest_period if date in base_period]
        return overlapping_dates

    def _remove_rest_periods(self, rest_periods, to_remove):
        new_list = [x for x in rest_periods if not to_remove]
        if len(new_list) == len(rest_periods):
            raise ValueError('Did not remove value from list, this would result in infinite loop')
        return new_list
