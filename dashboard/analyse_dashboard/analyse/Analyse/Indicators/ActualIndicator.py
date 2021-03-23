from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.DataIndicator import DataIndicator
from Analyse.Record.Record import Record


class ActualIndicator(DataIndicator, Aggregator):

    def __init__(self):
        self.collection = 'Data'
        self.graph_name = None

    def perform(self):
        """
        Main loop that applies business rules, aggregates resulting frame,
        and creates records for all projects in dataframe.

        Returns: RecordList with actual numbers for every project. Provider total is added in
        to_record.

        """
        df = self.aggregate(df=self.apply_business_rules(),
                            by='project')['sleutel']
        return self.to_record(df)

    def to_record(self, series):
        if not self.graph_name:
            raise NotImplementedError("Please use child class, graph name is derived from there.")

        result_dict = series.to_dict()
        result_dict['overview'] = series.sum()
        return Record(record=result_dict,
                      collection=self.collection,
                      client=self.client,
                      graph_name='HCopen')
