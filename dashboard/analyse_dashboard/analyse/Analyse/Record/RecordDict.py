from typing import MutableMapping

from Analyse.Record.Record import Record


class RecordDict(MutableMapping):
    """A Dictionary that holds all records for an analysis"""

    def __init__(self, record_collection=None):
        self.record_collection = {}
        if record_collection:
            for key, record in record_collection.items():
                if isinstance(record, Record):
                    self.record_collection[key] = record
                else:
                    raise ValueError(f"record collection must contain records,"
                                     f"{key} contains an object of type: {type(record)}")

    def add(self, key, record, RecordType, collection, **kwargs):
        self.record_collection[key] = RecordType(record, collection, **kwargs)

    def add_record_dict(self, key, record_dict):
        self.record_collection[key] = record_dict

    def to_firestore(self, client):
        record: Record
        for key, record in self.record_collection.items():
            record.to_firestore(key, client)

    def get_record(self, key):
        return self.record_collection[key].record

    def __getitem__(self, item) -> Record:
        return self.record_collection[item]

    def __setitem__(self, key, value):
        self.record_collection[key] = value

    def __delitem__(self, key):
        del self.record_collection[key]

    def __iter__(self):
        return iter(self.record_collection)

    def __len__(self):
        return len(self.record_collection)

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False

        key_union = set(self) & set(other)

        if not key_union == set(self) or not key_union == set(other):
            return False

        for key in key_union:
            if self[key] != other[key]:
                return False

        return self.record_collection == other.record_collection

    def __repr__(self):
        return f"{self.__class__.__qualname__}({str(self.record_collection)})"
