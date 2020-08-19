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

    def to_firestore(self, graph_name, client):
        document = firestore.Client().collection(self.collection).document(graph_name)
        firestore_dict = dict(record=self.record,
                              client=client,
                              graph_name=graph_name)
        document.set(firestore_dict)

    def __repr__(self):
        return f"{str(type(self)).rsplit('.')[-1][:-2]}(record={self.record}, collection={self.collection})"


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
    def to_firestore(self, graph_name, client):
        for k, v in self.record.items():
            document = firestore.Client().collection(self.collection).document(graph_name + "_" + k)
            print(f"Writing {graph_name + '_' + k} to {client}, in collection {self.collection}")
            document.set(dict(record=v,
                              client=client,
                              graph_name=graph_name,
                              project=k))
