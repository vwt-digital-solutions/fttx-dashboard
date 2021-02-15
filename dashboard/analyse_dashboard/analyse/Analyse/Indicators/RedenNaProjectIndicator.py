from Analyse.Indicators.Indicator import Indicator
from Analyse.PieChart import PieChart
from Analyse.Record.DictRecord import DictRecord


class RedenNaProjectIndicator(Indicator, PieChart):
    """
    Calculates reden na pie chart for every project
    """

    def apply_business_rules(self):
        """
        For this indicator we only need the cluster column, and sleutel column to count.
        Returns: Sliced dataframe containing only the relevant columns
        """
        return self.df[['cluster_redenna', 'sleutel']]

    def perform(self):
        """
        Aggregate to clusters and retrieve the counts, then make the result into a record.
        Returns: Record reday to be written to the firestore, containing clustered data.

        """
        aggregate = self.aggregate(
                                    df=self.apply_business_rules(),
                                    by=["project", "cluster_redenna"]
                                  )
        return self.to_record(aggregate)

    def to_record(self, df):

        for project, df in df.groupby('project'):
            self.to_pie_chart(df)
            graph_name = f"pie_na_{project}"
        dict_record = DictRecord(record=df,
                                 collection='Data',
                                 client=self.client,
                                 graph_name=graph_name)
        return dict_record
