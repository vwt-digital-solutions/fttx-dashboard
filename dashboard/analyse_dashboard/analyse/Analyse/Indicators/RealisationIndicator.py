import business_rules as br
import copy
import pandas as pd
from Analyse.Indicators.TimeseriesIndicator import TimeseriesIndicator
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


class RealisationIndicator(TimeseriesIndicator):
    """
    Indicator to calculate number of houses realised over days per project.
    Makes LineRecords per project, where all relevant details can be calculated.
    Also makes a LineRecords for the aggregate of the project lines for a given client
    """

    def __init__(self, project_info, project=None, **kwargs):
        super().__init__(**kwargs)
        self.project_info = project_info
        self.project = project
        self.type_total_amount = 'huisaansluitingen'
        self.indicator_name = 'RealisationIndicator'

    def apply_business_rules(self):
        """
        HC and HPend columns are needed, as we will calculate ratio between these two columns.
        Opleverdatum and project columns are used for aggregations.

        Returns: Sliced dataframe with only relevant columns.

        """
        df = copy.deepcopy(self.df)
        df = df[br.hpend(df)]
        df = df[['project', 'opleverdatum']]
        return df

    def perform(self):
        """
        Main perform to do all necessary calculations for indicator.

        Returns: List of Records with lines per project.

        """
        df = self.aggregate(df=self.apply_business_rules(),
                            by=['project', 'opleverdatum'],
                            agg_function='size')
        record_list = RecordList()
        line_client = None
        if self.project:
            data_for_loop = pd.concat({self.project: df.loc[self.project]}, names=['project']).groupby(level=0)
        else:
            data_for_loop = df.groupby(level=0)
        for project, timeseries in data_for_loop:
            if len(timeseries):
                line_project = TimeseriesLine(data=timeseries.droplevel(0),
                                              name=self.indicator_name,
                                              max_value=self.project_info[project][self.type_total_amount],
                                              project=project)
                record_list.append(self.to_record(line_project))
                line_client = self._add_line_to_line_client_aggregate(line_project, line_client)
        record_list.append(self.to_record(line_client))
        return record_list

    def to_record(self, line):
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

    def _add_line_to_line_client_aggregate(self, line, line_client=None):
        if line_client and line:
            line_client = line_client.add(line, fill_value=0)
        elif line:
            line_client = line
        if line_client:
            line_client.name = self.indicator_name
            line_client.project = self.client
        return line_client
