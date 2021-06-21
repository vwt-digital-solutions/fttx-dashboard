import logging
from abc import ABC, abstractmethod

logging.basicConfig(
    format=" %(asctime)s - %(name)s -%(levelname)s - %(filename)s:%(funcName)s:%(lineno)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger("Indicators")


class Indicator(ABC):
    def __init__(self, df, client, silence=False):
        """
        Basic implementation of indicator.
        Args:
            df: Dataframe with data to use.
            client: Client related to data, used in slicing and naming.
            silence: if silence, then logging is disabled. Used for nested indicators.
        """
        if not silence:
            logger.info(f"Performing {self.__class__.__name__} indicator")
        self.df = df
        self.client = client

    @abstractmethod
    def perform(self):
        ...
