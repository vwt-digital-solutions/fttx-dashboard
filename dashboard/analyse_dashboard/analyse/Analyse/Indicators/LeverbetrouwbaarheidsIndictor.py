from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.BusinessRule import BusinessRule
from datetime import datetime, timedelta
import copy
import business_rules as br
from Analyse.Record.DictRecord import DictRecord


class LeverbetrouwbaarheidIndicator(BusinessRule, Aggregator):
    """
    Class for leverbetrouwbaarheids indicator
    """
    def __init__(self, graph_name, **kwargs):
        super().__init__(**kwargs)
        self.graph_name = graph_name

    def apply_business_rules(self, project=None):
        """
        Business rule to calculate the ratio of houses that is connect in the last two weeks and is 'leverbetrouwbaar'
        according to the business rule.

        Args:
            project: project to calculate ratio for

        Returns: calculated ratio

        """
        df = copy.deepcopy(self.df)

        if project:
            df = df[df.project == project]

        # Select houses that are connected last two weeks
        mask = (
            (df.opleverdatum >= (datetime.today() - timedelta(weeks=2)))
            & (df.opleverdatum.notna())
        )
        df = df[mask]

        # Apply business rule for leverbetrouwbaar to be true or not
        mask = br.leverbetrouwbaar(df)
        ratio = mask.sum() / len(mask)

        return ratio

    def perform(self):
        """
        Main loop that applies business rules, and creates DictRecord for all projects in dataframe.

        Returns: DictRecord with ratios per project and the overall ratio.
        """
        project_dict = dict()
        project_dict['overall'] = self.apply_business_rules()

        for project in set(self.df.project):
            project_dict[project] = self.apply_business_rules(project)

        return self.to_records(project_dict)

    def to_record(self, project_dict):
        dict_record = DictRecord(
            record=project_dict,
            collection="Indicators",
            client=self.client,
            graph_name=self.graph_name,
        )
        return dict_record
