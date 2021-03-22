from Analyse.Indicators.InternalTargetIndicator import InternalTargetIndicator


class InternalTargetHPendIndicator(InternalTargetIndicator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.type_start_date = 'FTU0'
        self.type_end_date = 'FTU1'
        self.n_days_shift_end_date = 0
        self.type_speed = 'snelheid (m/week)'
        self.type_total_houses = 'huisaansluitingen'
        self.type_total_meters = 'meters BIS'
        self.indicator_name = 'InternalTargetHPendLine'
