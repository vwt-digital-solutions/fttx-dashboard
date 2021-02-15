from Analyse.Indicators.Indicator import Indicator
from Analyse.PieChart import PieChart
from Analyse.Record.Record import Record


class RedenNaOverviewIndicator(Indicator, PieChart):
    """
    Calculates reden na pie chart over all projects
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
                                    by="cluster_redenna"
                                  )
        return self.to_record(aggregate)

    def to_record(self, df):
        """
        Turn the data into a piechart, and then make it into a Record.
        Args:
            df: Clustered data to be turned into a Record

        Returns: Record containing all data.
        """
        record = self.to_pie_chart(df)
        return Record(record=record,
                      collection='Data',
                      client=self.client,
                      graph_name='reden_na_overview')
