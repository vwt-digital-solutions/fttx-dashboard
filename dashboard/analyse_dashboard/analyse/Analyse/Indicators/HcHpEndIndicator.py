import business_rules as br
from Analyse.Indicators.TimeseriesIndicator import TimeseriesIndicator
from Analyse.Aggregators.DateAggregator import DateAggregator
import copy
from Analyse.Capacity_analysis.Line import TimeseriesDistanceLine
from Analyse.Record.LineRecord import LineRecord


class HcHpEndIndicator(TimeseriesIndicator, DateAggregator):
    """
    Indicator to calculate HC/HPend ratios cumulative over days per project.
    Makes LineRecords per project, where all relevant details can be calculated.
    """

    def apply_business_rules(self):
        """
        HC and HPend columns are needed, as we will calculate ratio between these two columns.
        Opleverdatum and project columns are used for aggregations.

        Returns: Sliced dataframe with only relevant columns.

        """
        df = copy.deepcopy(self.df)
        df['HPend'] = br.hpend(df)
        df['HC'] = br.hc_opgeleverd(df)
        df = df[['HC', 'HPend', 'project', 'opleverdatum']]
        return df

    def perform(self):
        """
        Main perform to do all necessary calculations for HC/HPend indicator.

        Returns: List of Records with HC/HPend ratio per project.

        """
        df = self.aggregate(self.apply_business_rules())
        df['ratio'] = (df['HC'] / df['HPend'])
        records = []
        for project, timeseries in df.groupby(level=0)['ratio']:
            if len(timeseries):
                line = TimeseriesDistanceLine(timeseries.droplevel(0)).differentiate()
                records.append(self.to_record(line, project))
        return records

    def aggregate(self, df):
        """
        Aggregation is done in two steps: first, the amount of HC and HPend objects per day per project
        are calculated and resampled to contain every day in the date range.
        Second, The cumsum over time for each project is calculated and forward filed.
        Last, any NaN values at the start of the index are filled with 0's  to ensure we won't divide by nan later.

        Args:
            df: Dataframe containing columns opleverdatum, project, HC and HPend.

        Returns: Aggregated dataframe

        """
        summed_df = df.groupby(['project', 'opleverdatum']).agg({'HC': 'sum', 'HPend': 'sum'})
        return summed_df.groupby(level=0).cumsum()

    def to_record(self, line, project):
        """
        Loops over all projects in the dataframe column-wise, turns them into TimeseriesLines
        and turns the Lines into records.

        Args:
            df: Aggregated dataframe with ratio's.

        Returns: List of LineRecords.

        """
        return LineRecord(record=line,
                          collection='Lines',
                          graph_name='HcHpEndRatio',
                          phase='oplever',
                          client=self.client,
                          project=project,
                          to_be_integrated=True,
                          to_be_splitted_by_year=True)
