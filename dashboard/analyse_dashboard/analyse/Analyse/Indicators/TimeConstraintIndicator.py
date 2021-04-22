from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.ActualIndicator import ActualIndicator
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


class TimeConstraintIndicator(ActualIndicator, Aggregator):
    """
    Actualindicator that specifically deals with writing three different values to the firestore:
    on time, late and too late.
    """

    def perform(self):
        series = self.aggregate(df=self.apply_business_rules())
        records = RecordList()
        for project, time_values in series.iterrows():
            for time_constraint, value in time_values.iteritems():
                project_time_line = self.create_line(value)
                records.append(
                    self.to_record(project, project_time_line, time_constraint)
                )
        for time_constraint, values in series.iteritems():
            aggregate_line = self.create_line(values.sum())
            records.append(
                self.to_record("client_aggregate", aggregate_line, time_constraint)
            )
        return records

    def to_record(self, project, line, time_constraint):
        if not self.graph_name:
            raise NotImplementedError(
                "Please use child class, graph name is derived from there."
            )
        return LineRecord(
            line,
            collection="Indicators",
            graph_name=self.graph_name + time_constraint,
            to_be_normalized=False,
            phase="oplever",
            project=project,
            client=self.client,
        )
