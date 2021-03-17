from abc import ABC, abstractmethod


class Indicator(ABC):

    def __init__(self, df, client):
        self.df = df
        self.client = client

    @abstractmethod
    def perform(self):
        ...

    @abstractmethod
    def _add_line_to_list_of_records(self, df):
        ...
