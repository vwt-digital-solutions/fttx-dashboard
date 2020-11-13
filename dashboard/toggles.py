from featuretoggles import TogglesList


class ReleaseToggles(TogglesList):
    download_indicators: bool
    timeseries: bool


toggles = ReleaseToggles('toggles.yaml')
