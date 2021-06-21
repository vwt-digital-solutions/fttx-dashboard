from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.DataIndicator import DataIndicator
from Analyse.Record.DictRecord import DictRecord


class ActualRedenNAIndicator(DataIndicator, Aggregator):
    def __init__(self, **kwargs):
        """
        Used for modal download gegevens redenna.
        Args:
            **kwargs:
        """
        super().__init__(**kwargs)
        self.collection = "Indicators"
        self.graph_name = None
        self.relevant_columns = [
            "adres",
            "postcode",
            "huisnummer",
            "soort_bouw",
            "toestemming",
            "creation",
            "opleverstatus",
            "opleverdatum",
            "hasdatum",
            "redenna",
            "toelichting_status",
            "plan_type",
        ]

    def perform(self):
        """
        Aggregate to clusters and retrieve the counts, then make the result into a record.
        Returns: Record ready to be written to the firestore, containing clustered data.

        """
        df = self.apply_business_rules()
        aggregate = self.aggregate(df=df, by=["project", "cluster_redenna"]).fillna(0)
        project_dict = {}
        for project, _ in aggregate.groupby(level=0):
            clusters = aggregate.loc[project].to_dict()["sleutel"]
            sleutels = list(df[df.project == project].sleutel)
            df_aggregate = self.df[self.df.sleutel.isin(sleutels)][
                self.relevant_columns
            ]
            df_aggregate = df_aggregate.astype(str).to_dict(orient="records")
            project_dict[project] = dict(
                clusters=clusters,
                df_aggregate=df_aggregate,
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
