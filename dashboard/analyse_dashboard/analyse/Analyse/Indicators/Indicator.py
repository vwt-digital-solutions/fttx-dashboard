from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.BusinessRule import BusinessRule


class IndicatorBase:
    """
    Base class to be used for indicators, describes all fields that every step of the calculations of
    indicators can be used for.
    """
    def __init__(self, df, client):
        self.df = df
        self.client = client


class Indicator(BusinessRule, Aggregator):
    """
    Barebones indicator class containing standard functionality that every type of Indicator will be able to do.
    """
    def perform(self):

        self.to_record(
            self.aggregate(
                self.apply_business_rules()
            )
        )

    def to_record(self, df):
        raise NotImplementedError
