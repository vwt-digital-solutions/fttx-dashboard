from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.BusinessRule import BusinessRule
from Analyse.Indicators.Indicator import Indicator


class DataIndicator(Indicator, BusinessRule, Aggregator):
    """
    Barebones indicator class containing standard functionality that every type of Indicator will be able to do.
    """
    def calculate_line(self):
        raise NotImplementedError
