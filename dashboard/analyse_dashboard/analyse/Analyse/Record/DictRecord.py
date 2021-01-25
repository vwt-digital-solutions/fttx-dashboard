import logging

from google.cloud import firestore

from Analyse.Record.Record import Record


class DictRecord(Record):
    """DictRecord writes all items in the dictionary as a separate document to the collection.
    The key for the dict is used to fill the project field in the document."""

    def to_firestore(self):
        logging.info(f"Creating documents for {self.graph_name} in DictRecord")
        for k, v in self.record.items():
            document_name = f"{self.client}_{self.graph_name}_{k}"
            document = firestore.Client().collection(self.collection).document(document_name)
            logging.info(f"Set document {document_name}")
            document.set(self._to_document(graph_name=self.graph_name, client=self.client, record=v, project=k))

    def to_table_part(self):
        table_part = ""
        for k, v in self.record.items():
            document_name = f"{self.client}_{self.graph_name}_{k}"
            document = self._to_document(graph_name=self.graph_name, client=self.client, record=v, project=k)
            table_part += f"""<tr>
              <td>{document_name}</td>
              <td>{self.collection}</td>
              <td>{document}</td>
            </tr>"""
        return table_part

    def _to_document(self, graph_name, client, record="", project=""):
        return dict(record=record,
                    client=client,
                    graph_name=graph_name,
                    project=project)
