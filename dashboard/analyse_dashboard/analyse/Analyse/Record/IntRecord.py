import logging

from Analyse.Record.Record import Record, Validation


class IntRecord(Record):

    def _validate(self, value):
        validated = super()._validate(value)
        for i, el in enumerate(value):
            try:
                int(el)
            except ValueError:
                logging.warning(f"Element at index {i}:'{el}' is no integer")
                validated = False
                if self._validation == Validation.FAIL_AT_FIRST:
                    break
        return validated

    def _transform(self, record):
        return [int(el) for el in record]
