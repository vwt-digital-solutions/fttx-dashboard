from Analyse.Record.Record import Record


class ListRecord(Record):

    def _transform(self, record):
        transformed = {}
        for k, v in record.items():
            transformed[k] = list(v)
        return transformed
