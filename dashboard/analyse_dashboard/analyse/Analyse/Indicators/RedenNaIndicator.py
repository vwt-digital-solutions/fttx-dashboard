from Analyse.Capacity_analysis.Line import TimeseriesDistanceLine
from Analyse.Indicators.TimeseriesIndicator import TimeseriesIndicator
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


class RedenNaIndicator(TimeseriesIndicator):
    """
    Calculates reden na pie chart for every project
    """

    def apply_business_rules(self):
        """
        For this indicator we only need the cluster column, and sleutel column to count.
        Returns: Sliced dataframe containing only the relevant columns
        """
        return self.df[["project", "cluster_redenna", "hasdatum", "sleutel"]]

    def perform(self):
        """
        Aggregate to clusters and retrieve the counts, then make the result into records per project, per cluster.

        Returns: RecordList with a record for every project, per cluster, including provider level aggregates.

        """
        df = self.apply_business_rules()
        agg_df = self.aggregate(
            df=df, by=["project", "cluster_redenna", "hasdatum"], agg_function="size"
        )
        records = RecordList()
        for project, series in agg_df.groupby(level=0):
            for cluster, data in series.groupby(level=1):
                if len(data):
                    records.append(
                        self.to_record(
                            line=TimeseriesDistanceLine(data.droplevel([0, 1])),
                            project=project,
                            cluster=cluster,
                        )
                    )

        records = self.make_provider_lines(df, records)
        return records

    def make_provider_lines(self, df, records):
        """
            Makes provider level lines of the given dataframe, and adds them to the set of records
        Args:
            df: Dataframe sliced to contain all relevant data, through apply_business_rules
            records: RecordList that the records will be added to.

        Returns: RecordList with the added provider-level aggregate records.

        """
        for cluster, data in (
            df.groupby(["cluster_redenna", "hasdatum"]).size().groupby(level=0)
        ):
            if len(data):
                records.append(
                    self.to_record(
                        line=TimeseriesDistanceLine(data.droplevel(0)),
                        project="client_aggregate",
                        cluster=cluster,
                    )
                )
        return records

    def to_record(self, line, project, cluster):
        return LineRecord(
            record=line,
            collection="Indicators",
            graph_name=f"RedenNAindicator_{cluster}",
            phase="oplever",
            client=self.client,
            project=project,
            to_be_integrated=False,
            percentage=False,
            to_be_normalized=False,
        )
