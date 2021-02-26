from Analyse.Data import Data
import logging
import copy

logger = logging.getLogger('ETL')


class ETLBase:
    """

    Attributes:
        extracted_data (Data): A data object containing the extracted data sets.
        transformed_data (Data): A data object containing the transformed data sets.
    """

    def __init__(self, **kwargs):
        self.extracted_data = Data()
        self.transformed_data = Data()
        super().__init__()


class Extract(ETLBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def extract(self):
        logger.info("Extracting nothing the data in be base class")


class Transform(ETLBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def transform(self):
        if not self.transformed_data:
            logger.info("Transforming by using the extracted data directly. There was no previous tranformed data")
            # TODO make a copy. Now transformed_data and extracted_data point to the same object
            self.transformed_data = copy.deepcopy(self.extracted_data)
        else:
            logger.info("No tranformation in the base class as there was already transformed data")


class Load(ETLBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def load(self):
        logger.info("Loading nothing in the base class")


class ETL(Extract, Transform, Load):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def perform(self):
        self.extract()
        self.transform()
        self.load()

    def extract(self):
        super().extract()

    def transform(self):
        super().transform()

    def load(self):
        super().load()
