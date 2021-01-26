from Analyse.Record.Record import Record
import pandas as pd
from datetime import datetime, timedelta


# TODO: Documentation by Casper van Houten
class LineRecord(Record):
    """
    Record type that deals specifically with lines.
    Should be able to write different attributes of lines to firestore, given flags in init.

    Args:
        record:
        collection:
        client:
        graph_name:
        phase:
        **kwargs:
    """

    def __init__(self, record, collection, client, graph_name, phase, project, **kwargs):
        super().__init__(record, collection, client, graph_name, **kwargs)
        self.phase = phase
        self.project = project

    def _to_document(self):
        return dict(record=self.record,
                    client=self.client,
                    line=self.graph_name,
                    project=self.project,
                    phase=self.phase)

    def _transform(self, record):
        series = record.make_series().resample('W-MON').sum()
        series.index = series.index.format()
        record_to_write = {}
        record_to_write['series'] = series.to_dict()
        record_to_write['this_week'] = self.get_this_week_value(series)
        return record_to_write

    def get_this_week_value(self, series):
        first_day_this_week = pd.to_datetime(datetime.now() -
                                             timedelta(days=datetime.now().isoweekday() % 7 - 1)
                                             ).strftime('%Y-%m-%d')
        if first_day_this_week in series.index:
            value = series[first_day_this_week]
        else:
            value = 0
        return value

    def document_name(self, **kwargs):
        """
        Make document name based on client, phase and graph name.
        Args:
            **kwargs:

        Returns
            str: Document name as string.

        """

        return f'{self.client}_{self.phase}_{self.graph_name}'
