from Analyse.Record.Record import Record


class DateRecord(Record):
    """
    transforms a list of objects that have the strftime method and
    converts them to a string representation of the date"""

    def _transform(self, record):
        return [el.strftime('%Y-%m-%d') for el in record]
