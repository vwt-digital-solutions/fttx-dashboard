import base64
import io
import logging
import re

import pandas as pd
from xlrd import XLRDError

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
                f"Dit bestand heeft niet de juiste bestandsextensie. XLS of XLSX was verwacht, maar {extension} was "
                "aangetroffen."
            )
        try:
            self.parsed_file = pd.ExcelFile(io.BytesIO(self.file_content))
            return super().validate()
        except ValidationError as e:
            raise e
        except XLRDError:
            raise ValidationError("Bestand kan niet worden ingelezen. Het is mogelijk geen geldig excel bestand.")
        except Exception as e:
            logger.warning(f"An unexpected exception occured during reading of an excel file: {e}")
            raise ValidationError("Er is een probleem opgetreden bij het inlezen van het bestand.")


class XLSColumnValidator(XLSValidator):
    def __init__(self, columns, sheets=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parsed_file = None
        self.columns = columns
        self.sheets = sheets

    def validate(self):
        if super().validate():
            self.parsed_file = self.parsed_file.parse()
            if set(self.parsed_file.columns) != set(self.columns):
                unexpected = set(self.parsed_file.columns) - set(self.columns)
                missing = set(self.columns) - set(self.parsed_file.columns)
                error_message = "De kolommen in het bestand zijn niet juist."
                if unexpected:
                    error_message += f"\nOnverwacht aangetroffen in het bestand: {unexpected}"
                if missing:
                    error_message += f"\nNiet aangetroffen in het bestand: {missing}"
                raise ValidationError(error_message)
            return True


class XLSSheetValidator(XLSValidator):
    def __init__(self, sheets, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parsed_file = None
        self.sheets = sheets

    def validate(self):
        if super().validate():
            if not set(self.sheets).issubset(set(self.parsed_file.sheet_names)):
                missing = set(self.sheets) - set(self.parsed_file.sheet_names)
                error_message = "De sheets in het bestand zijn niet juist."
                if missing:
                    error_message += f"\nNiet aangetroffen in het bestand: {missing}"
                raise ValidationError(error_message)
            return True


class FileNameValidator(XLSSheetValidator):
    def __init__(self, patterns, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.patterns = patterns

    def validate(self):
        if super().validate():
            for pattern in self.patterns:
                match = re.findall(list(pattern.values())[0], self.file_name)
                if not match:
                    raise ValidationError(f"Ontbreekt in de filenaam: {list(pattern.keys())[0]}")
            return True
