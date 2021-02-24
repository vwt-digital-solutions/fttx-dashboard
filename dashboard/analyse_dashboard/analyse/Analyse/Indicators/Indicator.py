from Analyse.Indicators.IndicatorBase import IndicatorBase


class Indicator(IndicatorBase):

    def perform(self):
        raise NotImplementedError

    def to_record(self, df):
        raise NotImplementedError
