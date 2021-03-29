import logging
from abc import ABC, abstractmethod

logging.basicConfig(
    format=" %(asctime)s - %(name)s -%(levelname)s - %(filename)s:%(funcName)s:%(lineno)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger("Indicators")


class Indicator(ABC):
    def __init__(self, df, client, silence=False):
        if not silence:
            logger.info(f"Performing {self.__class__.__name__} indicator")
        self.df = df
        self.client = client

    @abstractmethod
    def perform(self):
        ...
