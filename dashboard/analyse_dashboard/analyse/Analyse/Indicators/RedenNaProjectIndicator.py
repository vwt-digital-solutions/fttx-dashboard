from Analyse.Indicators.TimeseriesIndicator import TimeseriesIndicator
from Analyse.PieChart import PieChart
from Analyse.Record.DictRecord import DictRecord


class RedenNaProjectDataIndicator(TimeseriesIndicator, PieChart):
    """
    Calculates reden na pie chart for every project
    """

    def apply_business_rules(self):
        """
        For this indicator we only need the cluster column, and sleutel column to count.
        Returns: Sliced dataframe containing only the relevant columns
        """
        return self.df[['project', 'cluster_redenna', 'sleutel']]

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
        project_dict = {}
        for project, df in df.groupby('project'):
            project_dict[project] = self.to_pie_chart(df)
        dict_record = DictRecord(record=project_dict,
                                 collection='Data',
                                 client=self.client,
                                 graph_name='reden_na_projects')
        return dict_record
