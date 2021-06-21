import copy

import business_rules as br
from Analyse.Capacity_analysis.Line import TimeseriesLine, concat
from Analyse.Indicators.TimeseriesIndicator import TimeseriesIndicator
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


class PlanningIndicatorDFN(TimeseriesIndicator):
    """
    Calculates the amount of houses planned per day over the entire period of the project for DFN projects.
    used in jaaroverzicht and maandoverzicht indicator
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.indicator_name = "PlanningHPendIndicator"

    def apply_business_rules(self):
        """
        For this indicator we only need the project and hasdatum column to count.
        Returns: Sliced dataframe containing only the relevant columns
        """
        df = copy.deepcopy(self.df)
        df = df[br.has_gepland(df)]
        df = df[["project", "hasdatum"]]
        return df

    def perform(self):
        """
        Main perform to do all necessary calculations for indicator.

        Returns: List of Records with lines per project and client_line for overall planning.

        """
        df = self.aggregate(
            df=self.apply_business_rules(),
            by=["project", "hasdatum"],
            agg_function="size",
        )

        line_list = []
        record_list = RecordList()
        if not df.empty:
            for project, df in df.groupby(level=0):
                if len(df):
                    line_project = self._make_project_line(project=project, df=df)
                    record_list.append(self.to_record(line_project))
                    line_list.append(line_project)

            line_client = concat(
                line_list, name=self.indicator_name, project="client_aggregate"
            )
            record_list.append(self.to_record(line_client))

        return record_list

    def _make_project_line(self, project, df):
        """
        calculates a TimeseriesLine for a given project
        Args:
            project (str): project name
            df (pd.DataFrame): dataframe containing planning of a project

        Returns: Timeseriesline with planning for the project

        """
        data = df.droplevel(level=0)
        return TimeseriesLine(data=data, name=self.indicator_name, project=project)

    def to_record(self, line):
        """
        Turn the data into LineRecord
        Args:
            line: Clustered data to be turned into a Record

        Returns: Record containing all data.
        """
        if line:
            record = LineRecord(
                record=line,
                collection="Indicators",
                graph_name=f"{line.name}",
                phase="oplever",
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
