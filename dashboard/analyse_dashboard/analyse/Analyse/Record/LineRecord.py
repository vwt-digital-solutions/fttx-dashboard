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

    def __init__(self, record, collection, client, graph_name, phase, **kwargs):
        super().__init__(record, collection, client, graph_name, **kwargs)
        self.phase = phase

    def _to_document(self, graph_name, client):
        return dict(record=self.record,
                    client=client,
                    line=graph_name,
                    phase=self.phase)

    def document_name(self, **kwargs):
        """
        Make document name based on client, phase and graph name.
        Args:
            **kwargs:

        Returns
            str: Document name as string.

        """
        graph_name = kwargs.get('graph_name')
        client = kwargs.get('client')
        return f'{client}_{self.phase}_{graph_name}'
