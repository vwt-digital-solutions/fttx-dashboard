import copy

import pandas as pd

import business_rules as br
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Indicators.TimeseriesIndicator import TimeseriesIndicator
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


class ConnectionTypeIndicator(TimeseriesIndicator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.collection = "Indicators"
        self.graph_name = "ConnectionTypeIndicator"

    def perform(self):
        """
        Aggregate to clusters and retrieve the counts of connection types per project,
        then make the result into a record.

        Returns: Record ready to be written to the firestore, containing clustered data.

        """
        df = self.apply_business_rules()
        project_aggregate = self.aggregate(
            df=df,
            by=["project", "afsluitcode", "afsluitdatum"],
            agg_function={"order_nummer": "count"},
        )

        record_list = RecordList()
        for project in list(project_aggregate.index.get_level_values(level=0).unique()):
            for afsluitcode in list(
                project_aggregate.loc[project].index.get_level_values(level=0).unique()
            ):
                line_project = TimeseriesLine(
                    data=pd.Series(
                        project_aggregate.loc[project].loc[afsluitcode]["order_nummer"]
                    ),
                    name=self.graph_name,
                    max_value=None,
                    project=project,
                )
                record_list.append(self.to_record(line_project, afsluitcode))

        return record_list

    def apply_business_rules(self):
        """
        HC and HPend columns are needed, as we will calculate ratio between these two columns.
        Opleverdatum and project columns are used for aggregations.

        Returns: Sliced dataframe with only relevant columns.

        """
        df = copy.deepcopy(self.df)
        df = df[br.mask_afsluitdatum_notna(df)]
        return df

    def to_record(self, line, afsluitcode):
        if line:
            record = LineRecord(
                record=line,
                collection="Indicators",
                graph_name=f"{line.name}",
                phase=afsluitcode,
                client=self.client,
                project=line.project,
                to_be_integrated=False,
                to_be_normalized=False,
                to_be_splitted_by_year=True,
                percentage=False,
            )
        else:
            record = None
        return record
