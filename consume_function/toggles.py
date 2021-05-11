from featuretoggles import TogglesList


class ReleaseToggles(TogglesList):
    upload: bool


toggles = ReleaseToggles('toggles.yaml')
