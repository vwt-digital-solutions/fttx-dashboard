import copy

from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Indicators.TimeseriesIndicator import TimeseriesIndicator
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


class PlanningIndicatorKPN(TimeseriesIndicator):
    """
    Base class for the planning indicator
    """

    def __init__(self, indicator_name, **kwargs):
        super().__init__(**kwargs)
        self.indicator_name = indicator_name

    def perform(self):
        """
        Main perform to do all necessary calculations for indicator.

        Returns: List of Records with line per project.

        """
        df = self.apply_business_rules()

        record_list = RecordList()
        for project, df in df.groupby(level=0):
            if len(df):
                line_project = self._make_project_line(project=project,
                                                       df=df)
                record_list.append(self.to_record(line_project))
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
        data = data[data.columns[0]]
        line_project = TimeseriesLine(data=data,
                                      name=self.indicator_name,
                                      project=project)
        return line_project

    def to_record(self, line):
        """
        Turns a TimeseriesLine into a record
        Args:
            line: TimeSeriesLine to be turned into a record

        Returns: LineRecord containing all data

        """
        if line:
            record = LineRecord(record=line,
                                collection='Lines',
                                graph_name=f'{line.name}',
                                phase='oplever',
                                client=self.client,
                                project=line.project,
                                to_be_integrated=False,
                                to_be_normalized=False,
                                to_be_splitted_by_year=True,
                                percentage=False)
        else:
            record = None
        return record


class PlanningHPEndIndicatorKPN(PlanningIndicatorKPN):
    """
    Calculates the HPEnd planning for KPN projects
    """

    def apply_business_rules(self):
        """
        Only the 'hp end' column is needed

        Returns: Sliced dataframe with only relevant columns.

        """
        df = copy.deepcopy(self.df)
        df = df[['hp end']]
        return df


class PlanningHPCivielIndicatorKPN(PlanningIndicatorKPN):
    """
    Calculates the HPCiviel planning for KPN projects
    """

    def apply_business_rules(self):
        """
        Only the 'hp civiel' column is needed

        Returns: Sliced dataframe with only relevant columns.

        """
        df = copy.deepcopy(self.df)
        df = df[['hp civiel']]
        return df
