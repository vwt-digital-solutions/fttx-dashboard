import base64
import io
import logging

import json
import pandas as pd

logger = logging.getLogger("Validator")


class ValidationError(Exception):
    pass


class Validator:

    def __init__(self, file_content, file_name, modified_date, **kwargs):
        self.maybe_content_type, self.content_string = file_content.split(',')
        self.file_content = base64.b64decode(self.content_string)
        self.file_name = file_name
        self.modified_date = modified_date
        logger.info(f"Got unexpected named arguments: {kwargs}")

    def validate(self):
        return True


class JSONValidator(Validator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.content_type = "application/json"
        self.parsed_file = None

    def validate(self):
        _, _, extension = self.file_name.rpartition(".")
        if "json" not in extension:
            raise ValidationError(
                f"File does not have the right extension. Expected .json but received: {extension}")
        try:
            json.loads(self.file_content)
        except json.decoder.JSONDecodeError as e:
            raise ValidationError(e)
        return super().validate()


class XLSValidator(Validator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parsed_file = None
        self.file_to_send = None
        self.content_type = "application/vnd.ms-excel"

    def validate(self):
        _, _, extension = self.file_name.rpartition(".")
        if "xls" not in extension:
            raise ValidationError(
                f"File does not have the right extension. Expected .xls or .xlsx but received: {extension}")

        try:

            self.parsed_file = pd.read_excel(io.BytesIO(self.file_content))
            return super().validate()
        except ValidationError as e:
            raise e
        except Exception:
            raise ValidationError("File can not be parsed as xls")


class XLSColumnValidator(XLSValidator):
    def __init__(self, columns, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parsed_file = None
        self.columns = columns

    def validate(self):
        if super().validate():
            if set(self.parsed_file.columns) != set(self.columns):
                unexpected = set(self.parsed_file.columns) - set(self.columns)
                missing = set(self.columns) - set(self.parsed_file.columns)
                error_message = "Mismatch in columns."
                if unexpected:
                    error_message += f"\nUnexpected in file: {unexpected}"
                if missing:
                    error_message += f"\nMissing in file: {missing}"
                raise ValidationError(error_message)
            return True
