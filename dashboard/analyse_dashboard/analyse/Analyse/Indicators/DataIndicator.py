from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.BusinessRule import BusinessRule


class DataIndicator(BusinessRule, Aggregator):
    """
    Barebones indicator class containing standard functionality that every type of Indicator will be able to do.
    """
    def perform(self):
        ...

    def to_record(self, df):
        ...
