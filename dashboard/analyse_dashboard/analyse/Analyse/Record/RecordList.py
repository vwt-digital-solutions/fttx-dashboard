class RecordList(list):
    """
    Extension of List class that creates a minimal extended collection of records.
    Can loop over all objects in the collection and write them to the firestore.
    """
    def to_firestore(self):
        [record.to_firestore() for record in self]
