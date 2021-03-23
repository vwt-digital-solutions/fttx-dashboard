import business_rules as br
from Analyse.Aggregators.DateAggregator import DateAggregator
import copy
from Analyse.Indicators.RatioIndicator import RatioIndicator
from Analyse.Record.LineRecord import LineRecord


class EightWeekRatioIndicator(RatioIndicator, DateAggregator):
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
        df['denominator'] = br.aangesloten_orders_tmobile(df)
        df['numerator'] = br.aangesloten_orders_tmobile(df, time_window='on time')
        df = df[['numerator', 'denominator', 'project', 'opleverdatum']]
        return df

    def to_record(self, line, project):
        """
        Turns a Line into a record

        Args:
            df: Aggregated dataframe with ratio's.

        Returns: List of LineRecords.

        """
        return LineRecord(record=line,
                          collection='Lines',
                          graph_name='8_week_ratio',
                          phase='oplever',
                          client=self.client,
                          project=project,
                          resample_method='mean',
                          to_be_integrated=True)
