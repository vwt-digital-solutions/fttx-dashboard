from Analyse.Indicators.InternalTargetIndicator import InternalTargetIndicator


class InternalTargetHPcivielIndicator(InternalTargetIndicator):
    def __init__(self, **kwargs):
        """Creates a line which contains the required daily speed (houses / day) for the phase "Civiel"
        given the targeted start date, end date and / or speed of the project.
        Used for indicator
        - Internal Target HP Civiel.
        - Target HP Civiel
        """
        super().__init__(**kwargs)
        self.type_start_date = "Civiel startdatum"
        self.type_end_date = "FTU1"
        self.n_days_shift_end_date = 63
        self.type_speed = "snelheid (m/week)"
        self.type_total_houses = "huisaansluitingen"
        self.type_total_meters = "meters BIS"
        self.indicator_name = "InternalTargetHPcivielLine"
