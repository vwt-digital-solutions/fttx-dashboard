import logging

from Analyse.FttX import FttXTransform

logger = logging.getLogger("FttX Analyse")


class TMobileTransform(FttXTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client_name = kwargs["config"].get("name")

    def transform(self, **kwargs):
        super().transform(**kwargs)
