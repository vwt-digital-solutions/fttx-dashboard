import base64
import io
import logging

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

            self.parsed_file = pd.read_excel(io.BytesIO(self.file_content))
            return super().validate()
        except ValidationError as e:
            raise e
        except XLRDError:
            raise ValidationError("Bestand kan niet worden ingelezen. Het is mogelijk geen geldig excel bestand.")
        except Exception as e:
            logger.warning(f"An unexpected exception occured during reading of an excel file: {e}")
            raise ValidationError("Er is een probleem opgetreden bij het inlezen van het bestand.")


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
                error_message = "De kolommen in het bestand zijn niet juist."
                if unexpected:
                    error_message += f"\nOnverwacht aangetroffen in het bestand: {unexpected}"
                if missing:
                    error_message += f"\nNiet aangetroffen in het bestand: {missing}"
                raise ValidationError(error_message)
            return True
