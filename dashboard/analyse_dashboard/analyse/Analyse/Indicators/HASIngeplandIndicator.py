from Analyse.Indicators.ActualIndicator import ActualIndicator
import business_rules as br
from Analyse.Aggregators.Aggregator import Aggregator


class HASIngeplandIndicator(ActualIndicator, Aggregator):

    def __init__(self):
        """
        Indicator to calculate current werkvoorraad
        """
        super().__init__()
        self.graph_name = 'has_ingepland'

    def apply_business_rules(self):
        """
        Slice all rows that are currently planned for HAS, and retrieve
        relevant columns to group on.

        Returns: Dataframe with relevant rows and columns

        """
        df = self.df[br.has_ingeplanned(self.df)]
        return df[['project', 'sleutel']]
