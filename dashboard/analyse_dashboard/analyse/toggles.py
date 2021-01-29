from featuretoggles import TogglesList


class ReleaseToggles(TogglesList):
    financial_view: bool
    upload: bool
    fc_sql: bool
    capacity_view: bool


toggles = ReleaseToggles('toggles.yaml')
