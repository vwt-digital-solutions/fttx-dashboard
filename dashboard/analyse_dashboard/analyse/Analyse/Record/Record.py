import logging
from enum import Enum

from google.cloud import firestore


class Validation(Enum):
    ALL = 1
    FAIL_AT_FIRST = 2


class Record:
    """
    Record class manages data that will be written to the firestore.
    :param record: data object
    :param collection: Firestore collection the data will be written to.
    :param client: (FttX) client the record contains data of.
    :param graph_name: The unique identifier of the data in the firestore. Not used for filtering
    :param validation: Type of validation to be used.
    """
    def __init__(self, record, collection, client, graph_name, validation=Validation.FAIL_AT_FIRST):
        self._record = None
        self._validation = validation
        self.client = client
        self.graph_name = graph_name
        self.record = record
        self.collection = collection

    @property
    def record(self):
        """
        Used to retrieve the data of the record object
        :return: record property of Record object.
        """
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

    def _to_document(self):
        return dict(record=self.record,
                    client=self.client,
                    graph_name=self.graph_name)

    def to_firestore(self):
        """
        Writes data of the record object into a collection of the firestore,
        including a client field.
        """
        document_name = self.document_name()
        document = firestore.Client().collection(self.collection).document(document_name)
        logging.info(f"Set document {document_name}")
        document.set(self._to_document())

    def document_name(self):
        """

        :return: Document name, made up of client and graph_name
        """
        return f"{self.client}_{self.graph_name}"

    def __repr__(self):
        return f"{str(type(self)).rsplit('.')[-1][:-2]}(record={self.record}, collection='{self.collection}')"

    def to_table_part(self, graph_name="", client=""):
        """
        Returns part of the HTML representation, function is called in _repr_html_
        Args:
            graph_name: graph name the table part needs to be made for
            client: Client of this particular graph

        Returns: part of the html representation of the record

        """
        document_name = self.document_name()
        return f"""<tr>
          <td>{document_name}</td>
          <td>{self.collection}</td>
          <td>{self._to_document()}</td>
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
