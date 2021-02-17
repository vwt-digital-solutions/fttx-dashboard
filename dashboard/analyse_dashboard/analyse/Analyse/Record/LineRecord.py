from Analyse.Record.Record import Record


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
        record_to_write = dict()
        record_to_write['series_week'] = record.get_line_aggregate(freq='W-MON',
                                                                   loffset='-1',
                                                                   aggregate_type='series',
                                                                   index_as_str=True).to_dict()
        record_to_write['series_month'] = record.get_line_aggregate(freq='MS',
                                                                    aggregate_type='series',
                                                                    index_as_str=True).to_dict()
        if record.name == 'werkvoorraad_absoluut_indicator':
            record_to_write['next_week'] = record.get_line_aggregate(freq='W-MON',
                                                                     loffset='-1',
                                                                     aggregate_type='value_mean')
            record_to_write['next_month'] = record.get_line_aggregate(freq='MS',
                                                                      aggregate_type='value_mean')
        else:
            record_to_write['next_week'] = record.get_line_aggregate(freq='W-MON',
                                                                     loffset='-1',
                                                                     aggregate_type='value_sum')
            record_to_write['next_month'] = record.get_line_aggregate(freq='MS',
                                                                      aggregate_type='value_sum')
        return record_to_write

    def document_name(self, **kwargs):
        """
        Make document name based on client, phase and graph name.
        Args:
            **kwargs:

        Returns
            str: Document name as string.

        """

        return f'{self.client}_{self.project}_{self.phase}_{self.graph_name}'
