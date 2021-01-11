import pandas as pd
import numpy as np

from Analyse.Capacity_analysis.Domain import DateDomain


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


