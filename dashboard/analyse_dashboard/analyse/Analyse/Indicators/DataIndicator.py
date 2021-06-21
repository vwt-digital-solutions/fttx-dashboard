from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.BusinessRule import BusinessRule


class DataIndicator(BusinessRule, Aggregator):
    """
    Barebone indicator class containing standard functionality that every type of Indicator will be able to do.
    Contains basic perform and to_record functions that are overwritten in specific indicators.
    """

    def perform(self):
        ...

    def to_record(self, df):
        ...
