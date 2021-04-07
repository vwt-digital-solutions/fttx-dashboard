import copy

import business_rules as br
from Analyse.Capacity_analysis.Line import TimeseriesLine, concat
from Analyse.Indicators.TimeseriesIndicator import TimeseriesIndicator
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


class InternalTargetTmobileIndicator(TimeseriesIndicator):
    """
    Indicator to calculate number of houses realised over days per project.
    Makes LineRecords per project, where all relevant details can be calculated.
    Also makes a LineRecords for the aggregate of the project lines for a given client
    """

    def __init__(self, project_info, **kwargs):
        super().__init__(**kwargs)
        self.project_info = project_info
        self.type_total_amount = "huisaansluitingen"
        self.columns = ["project", "creation"]
        self.indicator_name = "InternalTargetHPendLine"

    def apply_business_rules(self):
        """
        HC and HPend columns are needed, as we will calculate ratio between these two columns.
        Opleverdatum and project columns are used for aggregations.

        Returns: Sliced dataframe with only relevant columns.

        """
        df = copy.deepcopy(self.df)
        df = df[br.actieve_orders_tmobile(df)]
        df = df[self.columns]
        return df

    def perform(self):
        """
        Main perform to do all necessary calculations for indicator.

        Returns: List of Records with lines per project.

        """
        df = self.aggregate(
            df=self.apply_business_rules(), by=self.columns, agg_function="size"
        )

        line_list = []
        record_list = RecordList()
        if not df.empty:
            for project, timeseries in df.groupby(level=0):
                if len(timeseries):
                    line_project = TimeseriesLine(
                        data=timeseries.droplevel(0),
                        name=self.indicator_name,
                        max_value=self.project_info[project][self.type_total_amount],
                        project=project,
                    )
                    line_list.append(line_project)
                    record_list.append(self.to_record(line_project))

            line_client = concat(
                line_list, name=self.indicator_name, project="client_aggregate"
            )
            record_list.append(self.to_record(line_client))

        return record_list

    def to_record(self, line):
        if line:
            record = LineRecord(
                record=line,
                collection="Indicators",
                graph_name=f"{line.name}",
                phase="oplever",
                client=self.client,
                project="client_aggregate",
                to_be_integrated=False,
                to_be_normalized=False,
                to_be_splitted_by_year=True,
                percentage=False,
            )
        else:
            record = None
        return record
