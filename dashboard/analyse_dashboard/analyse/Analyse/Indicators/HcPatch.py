import copy

from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.ActualIndicator import ActualIndicator
import business_rules as br


class HcPatch(ActualIndicator, Aggregator):

    def __init__(self, **kwargs):
        """
        Indicator to calculate current werkvoorraad
        """
        super().__init__(**kwargs)
        self.graph_name = 'HCpatch'

    def apply_business_rules(self):
        df = copy.deepcopy(self.df)
        df['on time'] = br.hc_aanleg_tmobile(self.df, time_window='on time')
        df['limited'] = br.hc_aanleg_tmobile(self.df, time_window='limited')
        df['late'] = br.hc_aanleg_tmobile(self.df, time_window='late')
        return df[['project', 'on time', 'limited', 'late']]
