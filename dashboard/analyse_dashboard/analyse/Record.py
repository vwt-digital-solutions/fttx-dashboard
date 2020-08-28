from google.cloud import firestore
import logging
from enum import Enum


class Validation(Enum):
    ALL = 1
    FAIL_AT_FIRST = 2


# y_target_l - transform mbv ftu0 ftu1
# y_prog_l, y_target_l, rc1, rc2 - to list
# t_shift  - to str
# d_real_r, d_real_l_ri -> to list
# x_prog - to int
# x_d - to datetime
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
        document_name = f"{client}_{graph_name}"
        document = firestore.Client().collection(self.collection).document(document_name)
        document.set(self._to_document(graph_name, client))

    def __repr__(self):
        return f"{str(type(self)).rsplit('.')[-1][:-2]}(record={self.record}, collection={self.collection})"

    def to_table_part(self, graph_name="", client=""):
        document_name = f"{client}_{graph_name}"
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
    <th>Field</th>
    <th>Collection</th>
    <th>Document</th>
  </tr>
</thead>
<tbody>
{rows}
</tbody>
</table>"""
        return table


class IntRecord(Record):

    def _validate(self, value):
        validated = super()._validate(value)
        for i, el in enumerate(value):
            try:
                int(el)
            except ValueError:
                logging.warning(f"Element at index {i} is no integer")
                validated = False
                if self._validation == Validation.FAIL_AT_FIRST:
                    break
        return validated

    def _transform(self, record):
        return [int(el) for el in record]


class DateRecord(Record):
    """
    transforms a list of objects that have the strftime method and
    converts them to a string representation of the date"""

    def _transform(self, record):
        return [el.strftime('%Y-%m-%d') for el in record]


class ListRecord(Record):

    def _transform(self, record):
        transformed = {}
        for k, v in record.items():
            transformed[k] = list(v)
        return transformed


class StringRecord(Record):
    def _transform(self, record):
        transformed = {}
        for k, v in record.items():
            transformed[k] = str(v)
        return transformed


class DocumentListRecord(Record):
    """A list of dictionaries to be written as separate documents, no further manipulation needed. When  """

    def __init__(self, record, collection, document_key=None):
        if document_key is None:
            document_key = ['id']
        self.document_key = document_key
        super().__init__(record, collection)

    def _validate(self, value):
        validated = super()._validate(value)
        for i, doc in enumerate(value):
            if "record" not in doc:
                logging.warning(f"There is no field 'record' in document with index {i}")
                validated = False
                if self._validation == Validation.FAIL_AT_FIRST:
                    break

            if not self.document_key:
                logging.warning("You have no document key configured. This is needed to create a unique document name.")
                validated = False
                if self._validation == Validation.FAIL_AT_FIRST:
                    break

            for key_part in self.document_key:
                if key_part not in doc:
                    logging.warning(f'''There is no field '{key_part}' in document with index {i}.
                                        Your document must be uniquely identifiable''')
                    validated = False
                    if self._validation == Validation.FAIL_AT_FIRST:
                        break
        return validated

    def _document_name(self, document, client, graph_name):
        doc_name_parts = [client, graph_name] + [document[key_part] for key_part in self.document_key]
        document_name = "_".join(part for part in doc_name_parts if part)
        return document_name

    def to_firestore(self, graph_name=None, client=""):
        if not self.record:
            return

        db = firestore.Client()
        batch = db.batch()
        for i, document in enumerate(self.record):
            if client and "client" not in document:
                document['client'] = client
            fs_document = db.collection(self.collection).document(self._document_name(document, client, graph_name))
            batch.set(fs_document, document)
            if not i % 100:
                batch.commit()
                batch = db.batch()
        batch.commit()

    def to_table_part(self, graph_name="", client=""):
        table_part = ""
        for doc in self.record:
            document_name = self._document_name(doc, client, graph_name)
            table_part += f"""<tr>
              <td>{document_name}</td>
              <td>{self.collection}</td>
              <td>{doc}</td>
            </tr>"""
        return table_part


class DictRecord(Record):
    """DictRecord writes all items in the dictionary as a separate document to the collection.
    The key for the dict is used to fill the project field in the document."""

    def to_firestore(self, graph_name, client):
        for k, v in self.record.items():
            document_name = f"{client}_{graph_name}_{k}"
            document = firestore.Client().collection(self.collection).document(document_name)
            document.set(self._to_document(graph_name=graph_name, client=client, record=v, project=k))

    def to_table_part(self, graph_name="", client=""):
        table_part = ""
        for k, v in self.record.items():
            document_name = f"{client}_{graph_name}_{k}"
            document = self._to_document(graph_name=graph_name, client=client, record=v, project=k)
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


class RecordDict:
    """A Dictionary that holds all records for an analysis"""

    def __init__(self):
        self.record_collection = {}

    def add(self, key, record, RecordType, collection):
        self.record_collection[key] = RecordType(record, collection)

    def to_firestore(self, client):
        for key, record in self.record_collection.items():
            record.to_firestore(key, client)

    def items(self):
        return self.record_collection.items()