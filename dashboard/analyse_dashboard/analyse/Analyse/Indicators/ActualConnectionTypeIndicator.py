from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.DataIndicator import DataIndicator
from Analyse.Record.DictRecord import DictRecord


class ActualConnectionTypeIndicator(DataIndicator, Aggregator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.collection = "Indicators"
        self.graph_name = "ActualConnectionTypeIndicator"

    def perform(self):
        """
        Aggregate to clusters and retrieve the counts of connection types per project,
        then make the result into a record.

        Returns: Record ready to be written to the firestore, containing clustered data.

        """
        df = self.apply_business_rules()
        project_dict = self.aggregate(
            df=df, by=["project", "afsluitcode"], agg_function={"order_nummer": "count"}
        ).to_dict("series")

        return self.to_record(project_dict)

    def to_record(self, project_dict):
        dict_record = DictRecord(
            record=project_dict,
            collection="Indicators",
            client=self.client,
            graph_name=self.graph_name,
        )
        return dict_record
