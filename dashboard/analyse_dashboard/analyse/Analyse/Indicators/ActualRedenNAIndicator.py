from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.DataIndicator import DataIndicator
from Analyse.Record.DictRecord import DictRecord


class ActualRedenNAIndicator(DataIndicator, Aggregator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.collection = "Indicators"
        self.graph_name = None

    def perform(self):
        """
        Aggregate to clusters and retrieve the counts, then make the result into a record.
        Returns: Record ready to be written to the firestore, containing clustered data.

        """
        df = self.apply_business_rules()
        aggregate = self.aggregate(df=df, by=["project", "cluster_redenna"]).fillna(0)
        project_dict = {}
        for project, _ in aggregate.groupby(level=0):
            project_dict[project] = dict(
                clusters=aggregate.loc[project].to_dict()["sleutel"],
                sleutels=list(df[df.project == project].sleutel),
            )
        return self.to_record(project_dict)

    def to_record(self, project_dict):
        dict_record = DictRecord(
            record=project_dict,
            collection="Indicators",
            client=self.client,
            graph_name=self.graph_name,
        )
        return dict_record
