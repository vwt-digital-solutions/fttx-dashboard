from Analyse.Indicators.LineIndicator import LineIndicator


class InternalTargetIndicator(LineIndicator):

    def perform(self):
        list_project_lines = self._make_project_lines_from_dates_in_project_info()
        list_lines = self._add_client_aggregate_line_to_list_of_project_lines(list_project_lines)
        list_line_records = self._make_list_of_records_from_list_of_lines(list_lines)
        return list_line_records
