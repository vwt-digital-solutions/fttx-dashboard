from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.ActualIndicator import ActualIndicator
import business_rules as br


class HcOpenIndicator(ActualIndicator, Aggregator):

    def __init__(self):
        """
        Indicator to calculate current werkvoorraad
        """
        super().__init__()
        self.graph_name = 'HCopen'

    def apply_business_rules(self):
        df = self.df[br.openstaande_orders_tmobile(self.df)]
        df = df[['project', 'sleutel']]
        return df
