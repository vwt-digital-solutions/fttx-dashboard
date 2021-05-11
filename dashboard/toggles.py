from featuretoggles import TogglesList


class ReleaseToggles(TogglesList):
    financial_view: bool
    upload: bool


toggles = ReleaseToggles("toggles.yaml")
