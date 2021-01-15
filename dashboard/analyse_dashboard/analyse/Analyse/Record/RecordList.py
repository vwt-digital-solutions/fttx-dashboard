class RecordList(list):

    def to_firestore(self):
        [record.to_firestore() for record in self]
