from abc import ABC, abstractmethod


class Indicator(ABC):

    def __init__(self, df, client):
        self.df = df
        self.client = client

    @abstractmethod
    def perform(self):
        ...

    @abstractmethod
    def _make_list_of_records_from_list_of_lines(self, df):
        ...
