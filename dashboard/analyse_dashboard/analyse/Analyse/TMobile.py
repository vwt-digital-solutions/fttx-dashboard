import logging

import business_rules as br
from Analyse.FttX import FttXTransform
from functions import calculate_oplevertijd, wait_bins
from functions_tmobile import add_weeknumber

logger = logging.getLogger("FttX Analyse")


class TMobileTransform(FttXTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client_name = kwargs["config"].get("name")

    def transform(self, **kwargs):
        super().transform(**kwargs)
        self._HAS_add_weeknumber()
        self._georderd()
        self._opgeleverd()
        self._calculate_oplevertijd()
        self._waiting_category()

    def _georderd(self):
        # Iedere woning met een toestemmingsdatum is geordered door T-mobile.
        self.transformed_data.df["ordered"] = br.ordered(self.transformed_data.df)

    def _opgeleverd(self):
        # Iedere woning met een opleverdatum is opgeleverd.
        self.transformed_data.df["opgeleverd"] = br.opgeleverd(self.transformed_data.df)

    def _calculate_oplevertijd(self):
        # Oplevertijd is het verschil tussen de toestemmingsdatum en opleverdatum, in dagen.
        self.transformed_data.df["oplevertijd"] = self.transformed_data.df.apply(
            lambda x: calculate_oplevertijd(x), axis="columns"
        )

    def _HAS_add_weeknumber(self):
        self.transformed_data.df["has_week"] = add_weeknumber(
            self.transformed_data.df["hasdatum"]
        )

    def _waiting_category(self):
        toestemming_df = wait_bins(self.transformed_data.df)
        toestemming_df_prev = wait_bins(self.transformed_data.df, time_delta_days=7)
        self.transformed_data.df["wait_category"] = toestemming_df.bins
        self.transformed_data.df["wait_category_minus_delta"] = toestemming_df_prev.bins
