from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.BusinessRule import BusinessRule


class TimeseriesIndicator(BusinessRule, Aggregator):
    """
    Barebones indicator class containing standard functionality that Indicators based on timeseries will
    be able to do.
    Has functions such as calculate_line, perform and to_record to use in indicator calculations.
    """

    def calculate_line(self):
        raise NotImplementedError

    def perform(self):
        ...

    def to_record(self, df):
        ...
