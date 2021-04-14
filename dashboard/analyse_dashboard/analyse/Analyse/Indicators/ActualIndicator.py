import pandas as pd

from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Capacity_analysis.Domain import DateDomain
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Indicators.DataIndicator import DataIndicator
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


class ActualIndicator(DataIndicator, Aggregator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.collection = "Indicators"
        self.graph_name = None

    def aggregate(self, df):
        return super().aggregate(df=df, by="project", agg_function="sum")

    def perform(self):
        """
        Main loop that applies business rules, aggregates resulting frame,
        and creates records for all projects in dataframe.

        Returns: RecordList with actual numbers for every project. Provider total is added in
        to_record.

        """
        series = self.aggregate(df=self.apply_business_rules())
        records = RecordList()
        for project, value in series.iterrows():
            project_line = self.create_line(value)
            records.append(self.to_record(project, project_line))
        aggregate_line = self.create_line(series.sum())
        records.append(self.to_record("client_aggregate", aggregate_line))
        return records

    @staticmethod
    def create_line(value):
        """
        Creates a timseriesline from a single data point, on todays date.

        Args:
            value: value to be made into a timeseriesline

        Returns: a TimeseriesLine with index today and one value

        """
        domain = DateDomain(pd.datetime.today(), pd.datetime.today())
        return TimeseriesLine(domain=domain, data=value)

    def to_record(self, project, line):
        if not self.graph_name:
            raise NotImplementedError(
                "Please use child class, graph name is derived from there."
            )
        return LineRecord(
            line,
            collection="Indicators",
            graph_name=self.graph_name,
            to_be_normalized=False,
            phase="oplever",
            project=project,
            client=self.client,
        )
