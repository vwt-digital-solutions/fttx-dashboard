from Analyse.Indicators.IndicatorBase import IndicatorBase


class Aggregator(IndicatorBase):

    @staticmethod
    def aggregate(df, by, agg_function='count'):
        """

        Args:
            df: Dataframe to aggregate

            by: mapping, function, label, or list of labels
            Used to determine the groups for the groupby.
            If ``by`` is a function, it's called on each value of the object's
            index. If a dict or Series is passed, the Series or dict VALUES
            will be used to determine the groups (the Series' values are first
            aligned; see ``.align()`` method). If an ndarray is passed, the
            values are used as-is determine the groups. A label or list of
            labels may be passed to group by the columns in ``self``. Notice
            that a tuple is interpreted a (single) key.


            agg_function: function or dict
            Function to use for aggregating groups. If a function, must either
            work when passed a Panel or when passed to Panel.apply. If
            pass a dict, the keys must be DataFrame column names

        Returns:

        """
        return df.groupby(by=by).agg(agg_function)
