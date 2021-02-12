from Analyse.Aggregators.Aggregator import Aggregator
import pandas as pd


class DateAggregator(Aggregator):

    @staticmethod
    def aggregate(df, by, agg_function='count', freq='D'):

        by[0] = pd.Grouper(key=by[0],
                           freq=freq,
                           closed='left',
                           label="left")

        return df.groupby(by=by).agg(agg_function)
