from Analyse.Indicators.LineIndicator import LineIndicator


class InternalTargetIndicator(LineIndicator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.type_start_date = 'FTU0'
        self.type_end_date = 'FTU1'
        self.type_total_amount = 'huisaansluitingen'

    def perform(self):
        list_project_lines = self._make_project_lines_from_dates_in_project_info()
        list_lines = self._add_client_aggregate_line_to_list_of_project_lines(list_project_lines)
        list_line_records = self._make_list_of_records_from_list_of_lines(list_lines)
        return list_line_records
