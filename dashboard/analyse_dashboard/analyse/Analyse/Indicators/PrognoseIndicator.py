from Analyse.Indicators.LineIndicator import LineIndicator
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Capacity_analysis.Domain import DateDomainRange
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.RecordList import RecordList


class PrognoseIndicator(LineIndicator):

    def transform_dates_to_line(self, project):

        poc_real_rate_line = self._calculate_poc_real_rate_line(project)
        if (self.dates['date_FTU0'][project] is not None) & \
           (self.totals[project] is not None):
            slope = poc_real_rate_line.integrate().extrapolate(data_partition=0.5).slope
            # when there not enough realised data pionts, we take the average speed as slope
            if slope == 0:
                slope = self.average_slope
            distance_to_max_value = poc_real_rate_line.distance_to_max_value()
            daysleft = poc_real_rate_line.daysleft(slope=slope)
            # if there is work to do we extend the pocreal line, if not ideal line == realised line
            if distance_to_max_value > 0:
                domain = DateDomainRange(begin=poc_real_rate_line.domain.end, n_days=daysleft)
                line = poc_real_rate_line.append(TimeseriesLine(data=slope, domain=domain), skip=1)
            else:
                line = poc_real_rate_line
            # good to also use holiday periods here...do we copy paste the same functions?
            # holiday_periods = self.slice_holiday_periods(self.holiday_periods, poc_real_rate_line.domain.domain)
            # line = self.add_holiday_periods_to_line(line, holiday_periods)
        else:
            line = poc_real_rate_line
        line.name = 'PrognoseIndicator'
        line.max_value = self.totals[project]
        # yields in principle the same line as poc_verwacht_rate_line at phase capacity but without holiday periods
        return line

    def perform(self):
        record_list = RecordList()
        self._calculate_average_slope()
        for project in self.dates['date_FTU0']:
            line = self.transform_dates_to_line(project)
            record_list.append(self.to_record(line, project))
        return record_list

    def _calculate_average_slope(self):
        ds = self.df[~self.df.opleverdatum.isna()].opleverdatum
        ds_counted = ds.groupby(ds).count()
        self.average_slope = sum(ds_counted) / len(ds_counted)

    # replace this with line from RealisedIndicator?
    def _calculate_poc_real_rate_line(self, project):
        ds = self.df[(self.df.project == project) & (~self.df.opleverdatum.isna())].opleverdatum
        ds_counted = ds.groupby(ds).count()
        # this yields the same line as at phase capacity, do we want to use the same indicator line in the end?
        if not ds_counted.empty:
            line = TimeseriesLine(data=ds_counted,
                                  max_value=self.totals[project])
        else:
            # is this the best assumption??
            line = TimeseriesLine(data=0,
                                  max_value=self.totals[project])
        return line

    def to_record(self, line, project):
        return LineRecord(record=line,
                          collection='Lines',
                          graph_name=f'{line.name}',
                          phase='oplever',
                          client=self.client,
                          project=project)
