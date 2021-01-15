from Analyse.Record.Record import Record


class ListRecord(Record):
    """
    Extension of Record class that can transform record dictionaries of which values are list-like.
    """
    def _transform(self, record):
        transformed = {}
        for k, v in record.items():
            transformed[k] = list(v)
        return transformed
