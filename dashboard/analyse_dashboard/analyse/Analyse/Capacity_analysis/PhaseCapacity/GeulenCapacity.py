from Analyse.Capacity_analysis.PhaseCapacity.PhaseCapacity import PhaseCapacity


class GeulenCapacity(PhaseCapacity):
    def __init__(self):
        super().__init__()
        self.phase = 'geulen'
