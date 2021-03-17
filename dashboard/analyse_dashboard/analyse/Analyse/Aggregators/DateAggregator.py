from Analyse.Aggregators.Aggregator import Aggregator
import pandas as pd


class DateAggregator(Aggregator):

    @staticmethod
    def aggregate(df, by, agg_function='count'):

        by[0] = pd.Grouper(key=by[0],
                           freq='D',
                           closed='left',
                           label="left")

        return df.groupby(by=by).agg(agg_function)
