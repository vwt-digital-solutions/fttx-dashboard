import copy

from Analyse.Indicators.Indicator import Indicator


class BusinessRule(Indicator):
    """
    Basic implementation of Business Rules. A business Rule should return a sliced dataframe.
    The basic implementation is to return the complete slice, as a deepcopy to ensure no operations are performed
    on the original dataframe. Basic implemtnation of apply_business_rules.
    """

    def apply_business_rules(self):
        df = copy.deepcopy(self.df)
        return df
