import copy

import pandas as pd

import business_rules as br
from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Capacity_analysis.Domain import DateDomain
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Indicators.BusinessRule import BusinessRule
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


class LeverbetrouwbaarheidIndicator(BusinessRule, Aggregator):
    """
    Class for leverbetrouwbaarheids indicator
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.graph_name = "leverbetrouwbaarheid"

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

        return br.leverbetrouwbaar(df)

    def perform(self):
        """
        Main loop that applies business rules, and creates DictRecord for all projects in dataframe.

        Returns: DictRecord with ratios per project and the overall ratio.
        """
        records = RecordList()

        mask = self.apply_business_rules()
        aggregate_line = self.create_line(mask.sum() / len(mask))
        records.append(self.to_record("client_aggregate", aggregate_line))

        for project in set(self.df.project):
            mask = self.apply_business_rules()
            project_line = self.create_line(mask.sum() / len(mask))
            records.append(self.to_record(project, project_line))

        return records

    @staticmethod
    def create_line(value):
        """
        Creates a timseriesline from a single data point, on todays date.

        Args:
            value: value to be made into a timeseriesline

        Returns: a TimeseriesLine with index today and one value

        """
        domain = DateDomain(pd.datetime.today(), pd.datetime.today())
        return TimeseriesLine(domain=domain, data=value)

    def to_record(self, project, line):
        if not self.graph_name:
            raise NotImplementedError(
                "Please use child class, graph name is derived from there."
            )
        return LineRecord(
            line,
            collection="Indicators",
            graph_name=self.graph_name,
            to_be_normalized=False,
            phase="oplever",
            project=project,
            client=self.client,
        )
