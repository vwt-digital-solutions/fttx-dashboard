import pandas as pd
from datetime import timedelta
import numpy as np


class Domain:
    def __init__(self, begin, end):
        self.begin = begin
        self.end = end
        self.domain = range(begin, end)

    def shift(self, integer):
        new_begin = self.begin + integer
        new_end = self.end + integer
        return Domain(new_begin, new_end)

    def __call__(self):
        return self.domain

    def __len__(self):
        return len(self.domain)

    def __iter__(self):
        return range(self.begin, self.end)

    def get_range(self):
        return np.array(list(range(0, len(self.domain))))

    def get_intersect_index(self, value):
        raise NotImplementedError


class DateDomain(Domain):
    def __init__(self, begin, end):
        print(f'making domain between {begin}, {end}')
        self.begin = pd.to_datetime(begin)
        self.end = pd.to_datetime(end)
        self.domain = pd.date_range(start=begin,
                                    end=end,
                                    freq='D'
                                    )

    def shift(self, days):
        new_begin = self.begin + timedelta(days=days)
        new_end = self.end + timedelta(days=days)
        return DateDomain(begin=new_begin,
                          end=new_end
                          )

    def slice_domain(self, start_offset, stop_offset=0):
        return DateDomain(begin=self.begin + start_offset,
                          end=self.end + stop_offset)

    def get_intersect_index(self, value):
        return (value - self.begin).days


class Line:
    def __init__(self, name=None, label=None):
        self.name = name
        pass

    def make_series(self) -> pd.Series:
        '''
        Given attributes of the line,
        return a series with x-values as index, and y-value as values.
        '''
        raise NotImplementedError

    def make_normalised_series(self, total=None) -> pd.Series:
        '''
        Given the series from make_series and an optional total.
        If total is not given, use maximum x-value of series.
        return a series with x-values as index, and normalised y-values as values.

        '''
        series = self.make_series()
        return series / total

    def intersect(self, other):
        raise NotImplementedError

    def integrate(self):
        raise NotImplementedError

    def differentiate(self):
        raise NotImplementedError

    def translate_x(self, delta):
        '''
        Given a delta, shift the line object and return a new line object
        '''
        raise NotImplementedError

    def set_name(self, name):
        self.name = name


class LinearLine(Line):
    def __init__(self, slope, intersect, domain=None):
        self.slope = slope
        self.intersect = intersect
        self.domain = domain

    def make_series(self) -> pd.Series:
        '''
        Given attributes of the line,
        return a series with x-values as index, and y-value as values.
        '''
        if not self.domain:
            raise NotImplementedError
        values = self.slope * self.domain.get_range() + self.intersect
        series = pd.Series(index=self.domain.domain, data=values)
        return series

    def translate_x(self, delta):
        '''
        Given a delta, shift the line object and return a new line object
        '''
        translated_intersect = self.intersect - delta * self.slope
        new_domain = self.domain.shift(delta)
        translated_line = LinearLine(slope=self.slope,
                                     intersect=translated_intersect,
                                     domain=new_domain
                                     )

        return translated_line

    def get_most_recent_point(self, total=None):
        if total:
            recent_point = self.make_normalised_series(total)[-1:]
        else:
            recent_point = self.make_series()[-1:]
        return recent_point

    def focus_domain(self, lower_treshold=None, upper_treshold=np.Inf):
        if lower_treshold is not None:
            intersect = lower_treshold
        else:
            lower_treshold = -np.Inf
            intersect = self.intersect
        series = self.make_series()
        focused_series = series[(series >= lower_treshold) & (series <= upper_treshold)]
        domain = DateDomain(focused_series.index.min(), focused_series.index.max())
        return LinearLine(slope=self.slope, intersect=intersect, domain=domain)


class PointLine(Line):
    '''
    A point line is a collection of datapoints on a shared index.
    '''

    def __init__(self, data):
        self.data = data

    def translate_x(self, delta):
        '''
        Given a delta, shift the line object and return a new line object
        '''
        raise NotImplementedError

    def integrate(self):
        '''
        https://en.wikipedia.org/wiki/Numerical_integration
        '''
        # Temporarily use old cumsum method to mimic old implementation
        integral = self.make_series().cumsum()
        return PointLine(data=integral)

    def linear_regression(self, data_partition=None):
        '''
        Given a set of points, do a linear regression to extrapolate future data
        '''
        if data_partition:
            shift = int(len(self.domain) * data_partition)
            start = self.data.index[0] + shift
            end = self.data.index[-1]
            data = self.data[start:end]
            index = list(range(shift, len(data) + shift))

        else:
            index = list(range(0, len(self.data)))
            data = self.data
        slope, intersect = np.polyfit(index, data, 1)
        return slope, intersect


class TimeseriesLine(PointLine):
    '''
    A point line is a collection of datapoints on a shared datetime index.
    '''

    def __init__(self, data):
        self.data = data
        self.domain = DateDomain(data.index[0], data.index[-1])

    def make_series(self):
        filled_data = self.data.reindex(self.domain.domain, fill_value=0)
        return filled_data

    def extrapolate(self, data_partition=None):
        slope, intersect = self.linear_regression(data_partition)
        domain = DateDomain(self.data.index[0], self.data.index[-1])
        return LinearLine(slope=slope,
                          intersect=intersect,
                          domain=domain)

    def integrate(self):
        '''
        https://en.wikipedia.org/wiki/Numerical_integration
        '''
        # Temporarily use old cumsum method to mimic old implementation
        integral = self.make_series().cumsum()
        return TimeseriesLine(data=integral)


class ProjectGraph:
    def __init__(self, lines, xlabel=None, ylabel=None, date=None, limit=None):
        self.line_dict = {line.name: line for line in lines}
        pass

    def get(self, line):
        return self.line_dict.get(line)

    def _repr_html_(self):
        return '<h1>GRAPH<h1>'


class Project():

    def __init__(self):
        pass

    def get_project_lines(self, fase_delta, project_df, geulen_line=None):
        print('getting production per day')
        production_by_day = TimeseriesLine(project_df.groupby([project_df.index]).count())
        print('getting production over time')
        production_over_time = production_by_day.integrate()
        production_over_time.set_name('production_over_time')
        print('getting extrapolation')
        extrapolated_line = production_over_time.extrapolate()
        extrapolated_line.set_name('extrapolated_line')
        if geulen_line:
            forecast_line = geulen_line.translate_x(fase_delta)
            forecast_line.set_name('forecast_line')
        else:
            forecast_line = None
        required_production = None  # get_required_production(production_over_time, target_line)
        print('Combining and making graph')
        lines = [line for line in [production_over_time, extrapolated_line, forecast_line, required_production]
                 if line is not None]
        return ProjectGraph(lines=lines)
