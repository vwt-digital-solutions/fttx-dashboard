import copy

import business_rules as br
from Analyse.Aggregators.DateAggregator import DateAggregator
from Analyse.Capacity_analysis.Line import TimeseriesDistanceLine
from Analyse.Indicators.TimeseriesIndicator import TimeseriesIndicator
from Analyse.Record.LineRecord import LineRecord


class ConnectedOrdersTmobile(TimeseriesIndicator, DateAggregator):
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
        df["connected"] = br.aangesloten_orders_tmobile(df)
        df = df[br.aangesloten_orders_tmobile(df)]
        df = df[["connected", "project", "opleverdatum"]]
        return df

    def perform(self):
        df = self.apply_business_rules()
        agg_df = self.aggregate(df)
        records = []
        for project, timeseries in agg_df.groupby(level=0):
            line = TimeseriesDistanceLine(timeseries.droplevel(0)).differentiate()
            records.append(self.to_record(line, project))
        records.append(self.to_record(df, "client_aggregate"))

    def aggregate(df, by, date_index=0, agg_function="count"):
        summed_df = df.groupby(["project", "opleverdatum"]).agg(
            {"numerator": "sum", "denominator": "sum"}
        )
        return summed_df.groupby(level=0).cumsum()

    def to_record(self, line, project):
        """
        Turns a Line into a record

        Args:
            df: Aggregated dataframe with ratio's.

        Returns: List of LineRecords.

        """
        return LineRecord(
            record=line,
            collection="Indicators",
            graph_name="12_week_ratio",
            phase="oplever",
            client=self.client,
            project=project,
            resample_method="mean",
            to_be_integrated=True,
        )
