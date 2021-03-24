import copy
from Analyse.Indicators.ActualIndicator import ActualIndicator
import business_rules as br
from Analyse.Aggregators.Aggregator import Aggregator


class WerkvoorraadIndicator(ActualIndicator, Aggregator):

    def __init__(self, **kwargs):
        """
        Indicator to calculate current werkvoorraad
        """
        super().__init__(**kwargs)
        self.graph_name = 'werkvoorraad'

    def apply_business_rules(self):
        """
        Slice all rows that are currently in werkvoorraad, and retrieve
        the relevant columns to group on.

        Returns: DataFrame with relevant rows and columns.

        """
        df = copy.deepcopy(self.df)
        df['werkvoorraad'] = br.has_werkvoorraad(df)
        return df[['project', 'werkvoorraad']]
