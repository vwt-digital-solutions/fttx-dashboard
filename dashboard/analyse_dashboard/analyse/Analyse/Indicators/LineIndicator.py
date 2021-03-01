from Analyse.Indicators.Indicator import Indicator


class LineIndicator(Indicator):
    """
    Barebones indicator class containing standard functionality that every type of Indicator will be able to do.
    """

    def __init__(self, client, project_info, df):
        self.client = client
        self.project_info = project_info
        self.df = df

    def transform_dates_to_line(self):
        raise NotImplementedError
