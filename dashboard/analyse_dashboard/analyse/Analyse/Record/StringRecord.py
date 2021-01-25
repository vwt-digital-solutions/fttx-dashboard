from Analyse.Record.Record import Record


class StringRecord(Record):
    def _transform(self, record):
        transformed = {}
        for k, v in record.items():
            transformed[k] = str(v)
        return transformed
