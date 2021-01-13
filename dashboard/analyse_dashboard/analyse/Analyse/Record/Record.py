import logging
from enum import Enum

from google.cloud import firestore


class Validation(Enum):
    ALL = 1
    FAIL_AT_FIRST = 2


class Record:
    def __init__(self, record, collection, validation=Validation.FAIL_AT_FIRST):
        self._record = None
        self._validation = validation
        self.record = record
        self.collection = collection

    @property
    def record(self):
        return self._record

    @record.setter
    def record(self, value):
        if self._validate(value):
            self._record = self._transform(value)
        else:
            logging.warning("Record was not set, validation failed.")

    def _validate(self, value):
        return True

    def _transform(self, record):
        return record

    def _to_document(self, graph_name, client):
        return dict(record=self.record,
                    client=client,
                    graph_name=graph_name)

    def to_firestore(self, graph_name, client):
        document_name = self.document_name(graph_name=graph_name, client=client)
        document = firestore.Client().collection(self.collection).document(document_name)
        logging.info(f"Set document {document_name}")
        document.set(self._to_document(graph_name, client))

    def document_name(self, **kwargs):
        return f"{kwargs['client']}_{kwargs['graph_name']}"

    def __repr__(self):
        return f"{str(type(self)).rsplit('.')[-1][:-2]}(record={self.record}, collection='{self.collection}')"

    def to_table_part(self, graph_name="", client=""):
        document_name = self.document_name(graph_name=graph_name, client=client)
        return f"""<tr>
          <td>{document_name}</td>
          <td>{self.collection}</td>
          <td>{self._to_document(graph_name, client)}</td>
        </tr>"""

    def _repr_html_(self):
        rows = self.to_table_part()

        table = f"""<table>
<thead>
  <tr>
    <th>Document name</th>
    <th>Collection</th>
    <th>Document</th>
  </tr>
</thead>
<tbody>
{rows}
</tbody>
</table>"""
        return table

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False
        if self.record != other.record:
            return False
        if self.collection != other.collection:
            return False
        return True
