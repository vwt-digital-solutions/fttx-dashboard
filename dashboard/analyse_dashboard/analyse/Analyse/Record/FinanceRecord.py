from Analyse.Record.Record import Record


class FinanceRecord(Record):
    """
    FinanceRecord class that manage finance data that will be written to the firestore
    """
    def __init__(self, record, collection, client, graph_name, project, **kwargs):
        super().__init__(record=record, collection=collection, client=client, graph_name=graph_name, **kwargs)
        self.project = project

    def _to_document(self):
        return dict(record=self.record,
                    client=self.client,
                    graph_name=self.graph_name,
                    project=self.project)

    def document_name(self):
        """

        :return: Document name, made up of client and graph_name
        """
        return f"{self.client}_{self.project}_{self.graph_name}"
