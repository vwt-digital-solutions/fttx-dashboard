from Analyse.Indicators.DataIndicator import DataIndicator
import business_rules as br
from Analyse.Record.Record import Record
from Analyse.Aggregators.Aggregator import Aggregator


class HASIngeplandIndicator(DataIndicator, Aggregator):
    """
    Indicator to calculate current werkvoorraad
    """
    def apply_business_rules(self):
        """
        Slice all rows that are currently planned for HAS, and retrieve
        relevant columns to group on.

        Returns: Dataframe with relevant rows and columns

        """
        df = self.df[br.has_ingeplanned(self.df)]

        return df[['project', 'sleutel']]

    def perform(self):
        """
        Main loop that applies business rules, aggregates resulting frame,
        and creates records for all projects in dataframe.

        Returns: Recordlist with planned HAS for every project and overview.

        """
        df = self.aggregate(df=self.apply_business_rules(),
                            by=['project'])['sleutel']
        return self.to_record(df)

    def to_record(self, series):
        result_dict = series.to_dict()
        result_dict['overview'] = series.sum()
        return Record(record=result_dict,
                      collection='Data',
                      client=self.client,
                      graph_name='werkvoorraad')
