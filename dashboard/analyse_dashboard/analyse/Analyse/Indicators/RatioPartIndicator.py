from Analyse.Capacity_analysis.Line import TimeseriesDistanceLine
from Analyse.Indicators.TimeseriesIndicator import TimeseriesIndicator
from Analyse.Record.LineRecord import LineRecord


class RatioPartIndicator(TimeseriesIndicator):
    def perform(self):
        df = self.apply_business_rules()
        agg_df = self.aggregate(df)
        records = []
        for project, timeseries in agg_df.groupby(level=0)["indicator"]:
            if len(timeseries):
                line = TimeseriesDistanceLine(timeseries.droplevel(0)).differentiate()
                records.append(self.to_record(line, project))
        records.append(
            self.to_record(line=self._make_project_line(df), project="client_aggregate")
        )
        return records

    @staticmethod
    def _make_project_line(df):
        return TimeseriesDistanceLine(
            df.groupby("opleverdatum").sum()["indicator"]
        ).differentiate()

    @staticmethod
    def aggregate(df):
        summed_df = df.groupby(["project", "opleverdatum"]).sum().dropna()
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
            graph_name=self.graph_name,
            phase="oplever",
            client=self.client,
            project=project,
            to_be_integrated=True,
        )
