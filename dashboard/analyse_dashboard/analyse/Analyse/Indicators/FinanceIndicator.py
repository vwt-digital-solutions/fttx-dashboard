from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.BusinessRule import BusinessRule
from Analyse.Record.FinanceRecord import FinanceRecord
from Analyse.Record.RecordList import RecordList


class FinanceIndicator(BusinessRule, Aggregator):
    """
    Indicator to calculate the realised costs per project over time and in total. Also includes the
    budget.
    """
    def __init__(self, client, df_budget, df_actuals):
        self.client = client
        self.budget = df_budget
        self.actuals = df_actuals

    def apply_business_rules(self, project):
        """
        Function to return the relevant data for a specific project
        Args:
            project: the project for which the data is returned

        Returns: actuals: pd.DataFrame with financial realisation of a project
                 budget: pd.DataFrame with financial budget of a project

        """
        relevant_columns = ['kostendrager', 'categorie', 'sub_categorie', 'bedrag', 'project']
        budget = self.budget[self.budget.project_naam == project][relevant_columns]
        actuals = self.actuals[self.actuals.project_naam == project][relevant_columns + ['vastlegdatum']]
        return actuals, budget

    @staticmethod
    def aggregate(df):
        """
        Function to aggregate the realised costs of a project

        Args:
            df: pd.DataFrame with realised costs over time

        Returns: pd.DataFrame with aggregated costs per kostendrager

        """

        agg_project_df = df.groupby("kostendrager").agg({'categorie': 'first',
                                                         'sub_categorie': 'first',
                                                         'bedrag': 'sum'}).reset_index()
        return agg_project_df

    def perform(self):
        """
        Main perform function for the FinanceIndicator

        Returns: Recordlist with relevant records for the finance analyse

        """
        record_list = RecordList()
        for project in list(self.budget.project_naam.unique()):
            actuals, budget = self.apply_business_rules(project)
            if not actuals.empty and not budget.empty:
                actuals_aggregated = self.aggregate(actuals)
                record = dict(budget=budget.to_dict(orient='records'),
                              actuals=actuals.to_dict(orient='records'),
                              actuals_aggregated=actuals_aggregated.to_dict(orient='records'))
                record_list.append(self.to_record(record=record,
                                                  collection='Finance',
                                                  graph_name='finance_baan',
                                                  project=project))
        return record_list

    def to_record(self, record, collection, graph_name, project):
        """
        Function to transform records into FinanceRecords ready to load in the firestore
        Args:
            record: Dict with the data of the record
            collection: collection of the firestore for the record
            graph_name: graph_name of the record
            project: project of the record

        Returns: FinanceRecord

        """
        return FinanceRecord(record=record, collection=collection, client=self.client, graph_name=graph_name,
                             project=project)
