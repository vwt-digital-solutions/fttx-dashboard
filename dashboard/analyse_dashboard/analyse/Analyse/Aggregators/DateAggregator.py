from Analyse.Aggregators.Aggregator import Aggregator
import pandas as pd


class DateAggregator(Aggregator):

    @staticmethod
    def aggregate(df, by, date_index=0, agg_function='count'):

        by[date_index] = pd.Grouper(key=by[date_index],
                                    freq='D',
                                    closed='left',
                                    label="left")

        return df.groupby(by=by).agg(agg_function)
