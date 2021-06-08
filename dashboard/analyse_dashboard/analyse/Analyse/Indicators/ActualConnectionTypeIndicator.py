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
        project_aggregate = self.aggregate(
            df=df, by=["project", "afsluitcode"], agg_function={"order_nummer": "count"}
        )

        project_dict = {}
        for project in list(project_aggregate.index.get_level_values(level=0).unique()):
            project_dict[project] = project_aggregate.loc[project].to_dict()[
                "order_nummer"
            ]

        project_dict["client_aggregate"] = self.aggregate(
            df=df, by=["afsluitcode"], agg_function={"order_nummer": "count"}
        ).to_dict()["order_nummer"]

        return self.to_record(project_dict)

    def to_record(self, project_dict):
        dict_record = DictRecord(
            record=project_dict,
            collection="Indicators",
            client=self.client,
            graph_name=self.graph_name,
        )
        return dict_record
