from abc import ABC, abstractmethod


class Indicator(ABC):

    def __init__(self, df, client):
        self.df = df
        self.client = client

    @abstractmethod
    def perform(self):
        ...
