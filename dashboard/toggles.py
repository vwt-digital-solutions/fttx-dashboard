from featuretoggles import TogglesList


class ReleaseToggles(TogglesList):
    download_indicators: bool


toggles = ReleaseToggles('toggles.yaml')