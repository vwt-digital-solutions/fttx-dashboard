from featuretoggles import TogglesList


class ReleaseToggles(TogglesList):
    financial_view: bool
    upload: bool
    capacity_view: bool
    overview_indicators: bool
    leverbetrouwbaarheid: bool
    project_bis: bool
    transform_line_record: bool
    transform_frontend_newindicator: bool


toggles = ReleaseToggles("toggles.yaml")
