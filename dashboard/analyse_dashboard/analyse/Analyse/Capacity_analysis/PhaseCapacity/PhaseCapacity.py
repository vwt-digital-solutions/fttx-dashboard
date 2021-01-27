import pandas as pd
from datetime import timedelta
from Analyse.Capacity_analysis.Line import TimeseriesLine
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

    def __init__(self, df: pd.DataFrame, phases_config: dict, phase=None,
                 client=None, project=None, werkvoorraad=None, rest_dates=None):
        self.df = df
        self.phase = phase
        self.client = client
        self.project = project
        self.werkvoorraad = werkvoorraad
        self.phases_config = phases_config
        self.record_list = RecordList()
        self.rest_dates = rest_dates

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
        self.target_over_time = TimeseriesLine(data=pd.Series(data=self.phases_config['performance_norm_unit'],
                                                              index=DateDomainRange(begin=self.phases_config['start_date'],
                                                                                    n_days=self.phases_config['n_days']).domain),
                                               name='target_indicator')
        # calculate realised production over time
        if not isinstance(self.df.index[0], pd.Timestamp):
            ds = self.df[(~self.df.isna()) & (self.df <= pd.Timestamp.now())]
            ds = ds.groupby(ds).count()
        else:
            ds = self.df
        self.pocideal_real = TimeseriesLine(ds, domain=DateDomain(begin=ds.index[0], end=ds.index[-1]), name='poc_real_indicator')
        # calculate ideal production over time
        slope = (self.phases_config['total_units'] - self.pocideal_real.integrate().make_series().max()) / \
                (self.target_over_time.make_series().index[-1] - self.pocideal_real.make_series().index[-1]).days
        begin = self.pocideal_real.make_series().index[-1]
        end = self.target_over_time.make_series().index[-1]
        if end <= begin:
            end = begin + timedelta(7)
        pocideal_extrap = TimeseriesLine(data=pd.Series(data=slope, index=DateDomain(begin=str(begin), end=str(end)).domain))
        self.poc_ideal = TimeseriesLine(self.pocideal_real.make_series().add(pocideal_extrap.make_series().iloc[1:], fill_value=0),
                                        name='poc_ideal_indicator')
        # calculate ideal capacity over time
        self.capacity_ideal = self.poc_ideal / self.phases_config['phase_norm']
        self.capacity_ideal.name = 'capacity_ideal_indicator'

        # calculate werkvoorraad
        if self.phase == 'geulen':
            self.werkvoorraad = self.poc_ideal
        self.werkvoorraad.name = 'werkvoorraad_indicator'

        self.add_rest_periods_to_line(self.poc_ideal, self.rest_dates)

        # write indicators to records
        target_over_time_record = LineRecord(record=self.target_over_time,
                                             collection='Lines',
                                             graph_name=f'{self.target_over_time.name}',
                                             phase=self.phase,
                                             client=self.client,
                                             project=self.project)
        poc_ideal_over_time_record = LineRecord(record=self.poc_ideal,
                                                collection='Lines',
                                                graph_name=f'{self.poc_ideal.name}',
                                                phase=self.phase,
                                                client=self.client,
                                                project=self.project)
        capacity_over_time_record = LineRecord(record=self.capacity_ideal,
                                               collection='Lines',
                                               graph_name=f'{self.capacity_ideal.name}',
                                               phase=self.phase,
                                               client=self.client,
                                               project=self.project)
        werkvoorraad_over_time_record = LineRecord(record=self.werkvoorraad,
                                                   collection='Lines',
                                                   graph_name=f'{self.werkvoorraad.name}',
                                                   phase=self.phase,
                                                   client=self.client,
                                                   project=self.project)
        self.record_list.append(target_over_time_record)
        self.record_list.append(poc_ideal_over_time_record)
        self.record_list.append(capacity_over_time_record)
        self.record_list.append(werkvoorraad_over_time_record)
        return self

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
