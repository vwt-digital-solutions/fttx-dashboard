from Analyse.Indicators.InternalTargetIndicator import InternalTargetIndicator


class InternalTargetHPendIndicator(InternalTargetIndicator):
    def __init__(self, **kwargs):
        """Creates a line which contains the required daily speed (houses / day) for the phase "HAS"
        given the targeted start date, end date and / or speed of the project.
        Used for:
        - indicator Internal Target HP Civiel (overview)
        - target HPend (project)
        - jaaroverzicht.
        """
        super().__init__(**kwargs)
        self.type_start_date = "FTU0"
        self.type_end_date = "FTU1"
        self.n_days_shift_end_date = 0
        self.type_speed = "snelheid (m/week)"
        self.type_total_houses = "huisaansluitingen"
        self.type_total_meters = "meters BIS"
        self.indicator_name = "InternalTargetHPendLine"
