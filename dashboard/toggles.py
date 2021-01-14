from featuretoggles import TogglesList


class ReleaseToggles(TogglesList):
    download_indicators: bool
    timeseries: bool
    financial_view: bool
    upload: bool
    fc_sql: bool
    new_projectspecific_views: bool


toggles = ReleaseToggles('toggles.yaml')
