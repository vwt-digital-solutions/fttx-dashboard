import copy

import business_rules as br
from Analyse.Aggregators.DateAggregator import DateAggregator
from Analyse.Indicators.RatioIndicator import RatioIndicator
from Analyse.Record.LineRecord import LineRecord


class HcHpEndIndicator(RatioIndicator, DateAggregator):
    """
    Indicator to calculate HC/HPend ratios cumulative over days per project.
    Makes LineRecords per project, where all relevant details can be calculated.
    """

    def apply_business_rules(self):
        """
        Sets HC and HPend objects as numerator and denominator columns,
        as we will calculate ratio between these two columns.
        Opleverdatum and project columns are used for aggregations.

        Returns: Sliced dataframe with only relevant columns, including a numerator and denominator.

        """
        df = copy.deepcopy(self.df)
        df["denominator"] = br.hpend(df)
        df["numerator"] = br.hc_opgeleverd(df)
        df = df[["numerator", "denominator", "project", "opleverdatum"]]
        return df

    def to_record(self, line, project):
        """
        Loops over all projects in the dataframe column-wise, turns them into TimeseriesLines
        and turns the Lines into records.

        Args:
            df: Aggregated dataframe with ratio's.

        Returns: List of LineRecords.

        """
        return LineRecord(
            record=line,
            collection="Indicators",
            graph_name="HcHpEndRatio",
            phase="oplever",
            client=self.client,
            project=project,
            to_be_integrated=True,
        )
