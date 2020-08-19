from google.cloud import firestore


# y_target_l - transform mbv ftu0 ftu1
# y_prog_l, y_target_l, rc1, rc2 - to list
# t_shift  - to str
# d_real_r, d_real_l_ri -> to list
# x_prog - to int
# x_d - to datetime
class Record:
    def __init__(self, record, collection):
        self.record = None
        self.transform(record)
        self.collection = collection

    def transform(self, record):
        self.record = record

    def to_document(self, graph_name, client):
        return dict(record=self.record,
                    client=client,
                    graph_name=graph_name)

    def to_firestore(self, graph_name, client):
        document_name = f"{client}_{graph_name}"
        document = firestore.Client().collection(self.collection).document(document_name)
        document.set(self.to_document(graph_name, client))

    def __repr__(self):
        return f"{str(type(self)).rsplit('.')[-1][:-2]}(record={self.record}, collection={self.collection})"

    def to_table_part(self, graph_name, client):
        document_name = f"{client}_{graph_name}"
        return f"""<tr>
          <td>{document_name}</td>
          <td>{self.collection}</td>
          <td>{self.to_document(graph_name, client)}</td>
        </tr>"""


class IntRecord(Record):

    def transform(self, record):
        self.record = [int(el) for el in record]


class DateRecord(Record):

    def transform(self, record):
        self.record = [el.strftime('%Y-%m-%d') for el in record]


class ListRecord(Record):

    def transform(self, record):
        self.record = {}
        for k, v in record.items():
            self.record[k] = list(v)


class StringRecord(Record):
    def transform(self, record):
        self.record = {}
        for k, v in record.items():
            self.record[k] = str(v)


class DictRecord(Record):
    """DictRecord writes all items in the dictionary as a separate document to the collection."""

    def to_document(self, graph_name, client, record="", project=""):
        return dict(record=record,
                    client=client,
                    graph_name=graph_name,
                    project=project)

    def to_firestore(self, graph_name, client):
        for k, v in self.record.items():
            document_name = f"{client}_{graph_name}_{k}"
            document = firestore.Client().collection(self.collection).document(document_name)
            print(f"Writing {document_name} to {client}, in collection {self.collection}")
            document.set(self.to_document(graph_name=graph_name, client=client, record=v, project=k))

    def to_table_part(self, graph_name, client):
        table_part = ""
        for k, v in self.record.items():
            document_name = f"{client}_{graph_name}_{k}"
            document = self.to_document(graph_name=graph_name, client=client, record=v, project=k)
            table_part += f"""<tr>
              <td>{document_name}</td>
              <td>{self.collection}</td>
              <td>{document}</td>
            </tr>"""
        return table_part
