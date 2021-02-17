class IndicatorBase:
    """
    Base class to be used for indicators, describes all fields that every step of the calculations of
    indicators can be used for.
    """
    def __init__(self, df, client):
        self.df = df
        self.client = client
