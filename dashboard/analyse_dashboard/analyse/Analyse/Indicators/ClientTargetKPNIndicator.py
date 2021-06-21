from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.ActualIndicator import ActualIndicator


class ClientTargetKPNIndicator(ActualIndicator, Aggregator):
    def __init__(self, **kwargs):
        """
        Indicator to calculate current client target.
        Client target is a static value per year that is not saved in a centralized location.
        When this number is saved in a centralised location, this indicator should be updated.
        Used for indicator Client Target.
        """
        super().__init__(**kwargs)
        self.graph_name = "ClientTarget"
        self.client_target_value = 145000

    def perform(self):
        return self.to_record(
            "client_aggregate", self.create_line(self.client_target_value)
        )
