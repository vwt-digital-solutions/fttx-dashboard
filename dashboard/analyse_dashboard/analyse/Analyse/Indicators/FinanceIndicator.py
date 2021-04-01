from Analyse.Indicators.Indicator import Indicator
from Analyse.Record.FinanceRecord import FinanceRecord
from Analyse.Record.RecordList import RecordList


class FinanceIndicator(Indicator):
    """
    Indicator to calculate the realised costs per project over time and in total. Also includes the
    budget.
    """
    def __init__(self, client, df_budget, df_actuals):
        self.client = client
        self.budget = df_budget
        self.actuals = df_actuals

    def perform(self):
        record_list = RecordList()
        for project in list(self.budget.project_naam.unique()):
            actuals, budget = self._return_data_for_project(project)
            if not actuals.empty and not budget.empty:
                # actuals_aggregated = self._aggregate_actuals(actuals)
                record = dict(budget=budget.to_dict(orient='records'))
                #                             actuals=actuals.to_dict(orient='records'))
                #                             actuals_aggregated=actuals_aggregated.to_dict(orient='records'))
                record_list.append(self.to_record(record=record,
                                                  collection='Finance_test',
                                                  graph_name='finance_baan',
                                                  project=project))
        return actuals

    def to_record(self, record, collection, graph_name, project):
        return FinanceRecord(record=record, collection=collection, client=self.client, graph_name=graph_name,
                             project=project)

    @staticmethod
    def _aggregate_actuals(df):
        agg_project_df = df.groupby("kostendrager").agg({'categorie': 'first',
                                                         'sub_categorie': 'first',
                                                         'bedrag': 'sum'}).reset_index()
        return agg_project_df

    def _return_data_for_project(self, project):
        relevant_columns = ['kostendrager', 'categorie', 'sub_categorie', 'bedrag', 'project']
        budget = self.budget[self.budget.project_naam == project][relevant_columns]
        actuals = self.actuals[self.actuals.project_naam == project][relevant_columns + ['vastlegdatum']]
        return actuals, budget
