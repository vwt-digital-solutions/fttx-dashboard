from Analyse.Indicators.TimeseriesIndicator import TimeseriesIndicator
from Analyse.Aggregators.DateAggregator import DateAggregator
from Analyse.Capacity_analysis.Line import TimeseriesDistanceLine


class RatioIndicator(TimeseriesIndicator, DateAggregator):
    """
    Indicator to calculate HC/HPend ratios cumulative over days per project.
    Makes LineRecords per project, where all relevant details can be calculated.
    """

    def apply_business_rules(self):
        raise NotImplementedError

    def perform(self):
        """
        Main perform to do all necessary calculations for HC/HPend indicator.

        Returns: List of Records with HC/HPend ratio per project.

        """
        df = self.aggregate(self.apply_business_rules())
        df['ratio'] = (df['numerator'] / df['denominator'])
        records = []
        client_lines = []
        for project, timeseries in df.groupby(level=0)['ratio']:
            if len(timeseries):
                line = TimeseriesDistanceLine(timeseries.droplevel(0)).differentiate()
                client_lines.append(line)
                records.append(self.to_record(line, project))
        records.append(self.to_record(self._make_provider_level_line(df), 'overview'))
        return records

    @staticmethod
    def _make_provider_level_line(df):
        df = df.droplevel(0).groupby('opleverdatum').sum()
        ratio = df['numerator'] / df['denominator']
        return TimeseriesDistanceLine(ratio).differentiate()

    def aggregate(self, df):
        """
        Aggregation is done in two steps: first, the amount of numerator and denominator objects per day per project
        are calculated and resampled to contain every day in the date range.
        Second, The cumsum over time for each project is calculated and forward filed.
        Last, any NaN values at the start of the index are filled with 0's  to ensure we won't divide by nan later.

        Args:
            df: Dataframe containing columns opleverdatum, project, numerator and denominator.

        Returns: Aggregated dataframe

        """
        summed_df = df.groupby(['project', 'opleverdatum']).agg({'numerator': 'sum', 'denominator': 'sum'})
        return summed_df.groupby(level=0).cumsum()

    def to_record(self, line, project):
        raise NotImplementedError
