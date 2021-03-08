from Analyse.Indicators.TimeseriesIndicator import TimeseriesIndicator


class HPEndTargetDataIndicator(TimeseriesIndicator):

    def apply_business_rules(self):
        ...

    def perform(self):
        ...

    def to_record(self):
        ...

    def aggregate(df, by, agg_function='count'):
        ...
