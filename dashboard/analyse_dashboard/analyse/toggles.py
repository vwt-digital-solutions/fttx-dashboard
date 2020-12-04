from featuretoggles import TogglesList


class ReleaseToggles(TogglesList):
    download_indicators: bool
    timeseries: bool
    financial_view: bool
    consume_meters: bool
    upload: bool
    years_dropdown: bool


toggles = ReleaseToggles('toggles.yaml')
