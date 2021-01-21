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

    def __init__(self, df: pd.DataFrame, phases_config: dict, phase=None, client=None):
        self.df = df
        self.phase = phase
        self.client = client
        self.phases_config = phases_config
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

        # write indicators to records
        target_over_time_record = LineRecord(record=self.target_over_time,
                                             collection='Lines',
                                             graph_name=f'{self.client}+{self.phase}+{self.target_over_time.name}',
                                             phase=self.phase,
                                             client=self.client)
        poc_ideal_over_time_record = LineRecord(record=self.poc_ideal,
                                                collection='Lines',
                                                graph_name=f'{self.client}+{self.phase}+{self.poc_ideal.name}',
                                                phase=self.phase,
                                                client=self.client)
        capacity_over_time_record = LineRecord(record=self.capacity_ideal,
                                               collection='Lines',
                                               graph_name=f'{self.client}+{self.phase}+{self.capacity_ideal.name}',
                                               phase=self.phase,
                                               client=self.client)
        self.record_list.append(target_over_time_record).append(poc_ideal_over_time_record).append(capacity_over_time_record)
        return self

    # TODO: Documentation by Casper van Houten
    def get_record(self, **kwargs):
        return self.record_list
