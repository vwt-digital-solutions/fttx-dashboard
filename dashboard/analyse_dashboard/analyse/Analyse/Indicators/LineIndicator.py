from Analyse.Indicators.Indicator import Indicator


class LineIndicator(Indicator):
    """
    Barebones indicator class containing standard functionality that every type of Indicator will be able to do.
    """

    def __init__(self, client, dates, totals):
        self.client = client
        self.dates = dates
        self.totals = totals

    def transform_dates_to_line(self):
        raise NotImplementedError
