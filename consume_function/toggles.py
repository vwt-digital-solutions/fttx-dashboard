from featuretoggles import TogglesList


class ReleaseToggles(TogglesList):
    financial_view: bool
    upload: bool
    capacity_view: bool
    overview_indicators: bool
    project_bis: bool
    transform_line_record: bool


toggles = ReleaseToggles('toggles.yaml')
