from featuretoggles import TogglesList


class ReleaseToggles(TogglesList):
    download_indicators: bool
    timeseries: bool
    financial_view: bool
    upload: bool
    fc_sql: bool
    new_structure_overviews: bool
    new_structure_overviews_graphs: bool
    old_structure_overview_boxes: bool
    old_structure_overview_graphs: bool


toggles = ReleaseToggles('toggles.yaml')
