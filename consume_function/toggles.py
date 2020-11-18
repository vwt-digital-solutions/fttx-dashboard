from featuretoggles import TogglesList


class ReleaseToggles(TogglesList):
    download_indicators: bool
    timeseries: bool
    financial_view: bool
    consume_meters: bool
    upload: bool


toggles = ReleaseToggles('toggles.yaml')
