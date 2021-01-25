from typing import MutableMapping

from Analyse.Record.Record import Record
from Analyse.Record.RecordList import RecordList


class RecordListWrapper(MutableMapping):
    """A Dictionary that holds all records for an analysis"""

    def __setitem__(self, k, v) -> None:
        pass

    def __delitem__(self, v) -> None:
        pass

    def __getitem__(self, k):
        pass

    def __len__(self) -> int:
        return len(self.record_list)

    def __iter__(self):
        return self.record_list.__iter__()

    def __init__(self, client, record_collection=None):
        self.record_list = RecordList()
        self.client_name = client
        if record_collection:
            for key, record in record_collection.items():
                if isinstance(record, Record):
                    self.record_collection[key] = record
                else:
                    raise ValueError(f"record collection must contain records,"
                                     f"{key} contains an object of type: {type(record)}")

    def add(self, key, record, record_type, collection, **kwargs):
        """
        Function to add data to the RecordListWrapper. Will create a record of given type
        and add it to the collection.
        Args:
            key: Name of Record
            record: Data that will be made into a record
            record_type: type of Record that the data will be turned into.
            collection: Collection the record should be part of.
            **kwargs:
        """
        record = record_type(record, collection, self.client_name, key, **kwargs)
        self.record_list.append(record)

    def to_firestore(self):
        """
        Calls the to_firestore function of all objects in its collection, writing the
        entire collection to the firestore.
        """
        self.record_list.to_firestore()
