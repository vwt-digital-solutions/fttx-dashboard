"""
Finance_ETL.py
============

The ETL process for FttX Finance analyse.
"""
from Analyse.ETL import Extract, ETL, Transform, Load


class FinanceExtract(Extract):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def extract(self):
        super().extract()
        self.extract_baan_budget_from_sql()
        self.extract_baan_realisation_from_sql()
        self.extract_categorisering()

    def extract_baan_budget_from_sql(self):
        ...

    def extract_baan_realisation_from_sql(self):
        ...

    def extract_categorisering(self):
        ...


class FinanceTransform(Transform):

    def __init__(self, **kwargs):
        super().__init__(self, **kwargs)

    def transform(self):
        super().transform()
        self.transform_baan_budget()
        self.transform_baan_realisation()
        self.transform_categorisering()

    def transform_baan_budget(self):
        ...

    def transform_baan_realisation(self):
        ...

    def transform_categorisering(self):
        ...


class FinanceLoad(Load):

    def __init__(self, **kwargs):
        super().__init__(self, **kwargs)

    def load(self):
        super().load()
        self.load_baan_to_firestore()

    def load_baan_to_firestore(self):
        ...


class FinanceETL(ETL, FinanceExtract, FinanceTransform):

    def __init__(self, **kwargs):
        super().__init__(self, **kwargs)
