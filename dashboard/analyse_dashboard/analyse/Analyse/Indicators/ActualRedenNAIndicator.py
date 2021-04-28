from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.DataIndicator import DataIndicator
from Analyse.Record.DictRecord import DictRecord
from Analyse.Record.RecordList import RecordList


class ActualRedenNAIndicator(DataIndicator, Aggregator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.collection = "Indicators"
        self.graph_name = None

    def perform(self):
        """
        Aggregate to clusters and retrieve the counts, then make the result into a record.
        Returns: Record reday to be written to the firestore, containing clustered data.

        """
        df = self.apply_business_rules()
        aggregate = self.aggregate(df=df, by=["project", "cluster_redenna"]).fillna(0)
        records = RecordList()
        for project, _ in aggregate.groupby(level=0):
            project_dict = dict(
                clusters=aggregate.loc[project].to_dict()["sleutel"],
                sleutels=list(df[df.project == project].sleutel),
            )
            records.append(self.to_record(project, project_dict))

        return records

    def to_record(self, project, project_dict):
        dict_record = DictRecord(
            record=project_dict,
            collection="Indicators",
            client=self.client,
            graph_name=self.graph_name,
        )
        return dict_record
