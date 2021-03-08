from Analyse.Indicators.DataIndicator import DataIndicator
import business_rules as br
import copy
from Analyse.Record.Record import Record


class HcHpEndIndicator(DataIndicator):
    """
    Indicator to
    """
    def apply_business_rules(self):
        df = copy.deepcopy(self.df)
        df['HPend'] = br.hpend(df)
        df['HC'] = br.hc_opgeleverd(df)
        df = df[['HC', 'HPend', 'project']]
        return df

    def perform(self):
        df = self.aggregate(df=self.apply_business_rules(),
                            by='project',
                            agg_function='sum')
        ratio = (df['HC'] / df['HPend']).fillna(0)
        return self.to_record(ratio)

    def to_record(self, series):
        record = series.to_dict()
        return Record(record=record,
                      collection='Data',
                      client=self.client,
                      graph_name='ratio_hc_hpend')
