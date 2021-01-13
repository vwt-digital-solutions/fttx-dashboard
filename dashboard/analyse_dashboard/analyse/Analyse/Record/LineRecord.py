from Analyse.Record.Record import Record


class LineRecord(Record):

    def __init__(self, phase, **kwargs):
        super().__init__(**kwargs)
        self.phase = phase

    def _to_document(self, graph_name, client):

        return dict(record=self.record,
                    client=client,
                    line=graph_name,
                    phase=self.phase)

    def document_name(self, **kwargs):
        graph_name = kwargs.get('graph_name')
        client = kwargs.get('client')
        return f'{client}_{self.phase}_{graph_name}'
