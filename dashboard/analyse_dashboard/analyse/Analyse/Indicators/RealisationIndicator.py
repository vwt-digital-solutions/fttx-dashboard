import business_rules as br
import copy
from Analyse.Indicators.TimeseriesIndicator import TimeseriesIndicator
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList
from Analyse.Capacity_analysis.Line import concat


class RealisationIndicator(TimeseriesIndicator):
    """
    Indicator to calculate number of houses realised over days per project.
    Makes LineRecords per project, where all relevant details can be calculated.
    Also makes a LineRecords for the aggregate of the project lines for a given client
    """

    def __init__(self, project_info, return_lines=False, **kwargs):
        super().__init__(**kwargs)
        self.project_info = project_info
        self.return_lines = return_lines
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

        line_list = []
        record_list = RecordList()
        for project, timeseries in df.groupby(level=0):
            if len(timeseries):
                line_project = TimeseriesLine(data=timeseries.droplevel(0),
                                              name=self.indicator_name,
                                              max_value=self.project_info[project][self.type_total_amount],
                                              project=project)
                line_list.append(line_project)
                record_list.append(self.to_record(line_project))

        if line_list:
            line_client = concat(line_list, name=self.indicator_name, project=self.client)
            line_list.append(line_client)
            record_list.append(self.to_record(line_client))
        output_list = record_list
        if self.return_lines:
            output_list = line_list
        return output_list

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
