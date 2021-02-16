"""
Line.py
=====================
A module to work and calculate with Lines.

This module can be used to perform calculations with lines such as calculating integrals, derivatives.
Add, subtract, mulitply and divide lines. These operations can be perfomed between lines, or with a line and a number.

Examples
========

>>> line = PointLine([1,2,3,4])
>>> line
0    1
1    2
2    3
3    4
dtype: int64


>>> line.integrate()
0     1
1     3
2     6
3    10
dtype: int64

>>> line + 1
0    2
1    3
2    4
3    5
dtype: int64

>>> line + line
0    2
1    4
2    6
3    8
dtype: int64

"""

import base64
from io import BytesIO

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import timedelta

from Analyse.Capacity_analysis.Domain import DateDomain, Domain


class Line:
    """
    This is the base class for all `Line` objects.

    Args:
        name (str):  optional, a name for the line
        label (str): optional
    """

    def __init__(self, name="", label=None):
        self.name = name
        self.label = label

    def make_series(self) -> pd.Series:
        """
        Given attributes of the line, return a series with x-values as index, and y-value as values.

        Returns:
            pd.Series: A pandas Series of the function within the defined domain
        """
        raise NotImplementedError

    def make_normalised_series(self, maximum=None, percentage=False) -> pd.Series:
        """
         A function to return a normalized series of the line.

        Args:
            maximum (float, int): optional, if maximum is not given, use maximum x-value of series. Otherwise use the provided
                      maximum

        Returns:
            pd.Series: A pandas Series of the function within the defined domain where the values are normalized
        """
        series = self.make_series()
        if not maximum:
            maximum = max(series)
        if percentage:
            normalized_series = series / maximum * 100
        else:
            normalized_series = series / maximum
        return normalized_series

    # TODO: Documentation by Casper van Houten
    def get_most_recent_point(self, total=None):
        if total:
            recent_point = self.make_normalised_series(total)[-1]
        else:
            recent_point = self.make_series()[-1]
        return recent_point

    def intersect(self, other):
        raise NotImplementedError

    def integrate(self):
        raise NotImplementedError

    def differentiate(self):
        raise NotImplementedError

    def translate_x(self, delta):
        """
        Given a delta, shift the line object and return a new line object

        Args:
            delta:

        Returns:

        """
        raise NotImplementedError

    def __add__(self, other):
        raise NotImplementedError

    def __iadd__(self, other):
        raise NotImplementedError

    def __sub__(self, other):
        raise NotImplementedError

    def __isub__(self, other):
        raise NotImplementedError

    def __mul__(self, other):
        raise NotImplementedError

    def __imul__(self, other):
        raise NotImplementedError

    def __truediv__(self, other):
        raise NotImplementedError

    def __idiv__(self, other):
        raise NotImplementedError

    def set_name(self, name):
        self.name = name

    def _notebook_name(self):
        return self.__str__()

    def __str__(self):
        return self.name

    def _repr_html_(self):
        fig = plt.figure()
        series = self.make_series()
        series.plot()
        tmpfile = BytesIO()
        fig.savefig(tmpfile, format='png')
        plt.close()
        encoded = base64.b64encode(tmpfile.getvalue()).decode('utf-8')
        html = f"{self._notebook_name()}<br/><img src=\'data:image/png;base64,{encoded}\'>"
        return html


class FunctionLine(Line):
    """
    A function line is defined by a mathematical function
    """
    ...


class LinearLine(FunctionLine):
    """
    A linear line is defined by the slope and the y-intercept.

    https://en.wikipedia.org/wiki/Linear_equation#Slope%E2%80%93intercept_form

    Args:
        slope (float, int): The slope of line
        intercept (float, int): The y coordinate of the lines intersection with the y-axis
        domain (Domain): The domain for which the line is defined.
    """

    def __init__(self, slope: float, intercept: float, domain: Domain = None, *args, **kwargs):
        self.slope = slope
        self.intercept = intercept
        self.domain = domain
        super().__init__(*args, **kwargs)

    def _notebook_name(self):
        name = "<br/>".join(x for x in [self.name, f"$$f(x) = {self.slope} \\cdot x + {self.intercept}$$"] if x)
        return name

    def __str__(self):
        return f"f(x) = {self.slope} * x + {self.intercept}"

    def __repr__(self):
        return f"{self.__class__.__name__}(slope={self.slope}, intercept={self.intercept}, domain={self.domain}," \
               f" name='{self.name}')"

    def make_series(self) -> pd.Series:
        """
        Given attributes of the line, return a series with x-values as index, and y-values as values.

        Returns:
            pd.Series: A pandas Series of the function within the defined domain

        Example
        --------
        >>> line = LinearLine(slope=3, intercept=1, domain=Domain(0,10))
        >>> line.make_series()
        0     1
        1     4
        2     7
        3    10
        4    13
        5    16
        6    19
        7    22
        8    25
        9    28
        dtype: int64
        """
        if not self.domain:
            raise NotImplementedError(f"Can not create a series for a {self.__class__.__name__} when no domain is "
                                      f"specified")
        values = self.slope * self.domain.get_range() + self.intercept
        series = pd.Series(index=self.domain.domain, data=values)
        return series

    # TODO: Documentation by Casper van Houten
    def translate_x(self, delta):
        translated_intersect = self.intercept - delta * self.slope
        new_domain = self.domain.shift(delta)
        translated_line = LinearLine(slope=self.slope,
                                     intercept=translated_intersect,
                                     domain=new_domain
                                     )

        return translated_line

    # TODO: Documentation by Casper van Houten
    def focus_domain(self, lower_treshold=None, upper_treshold=np.Inf):
        if lower_treshold is not None:
            intersect = lower_treshold
        else:
            lower_treshold = -np.Inf
            intersect = self.intercept
        series = self.make_series()
        focused_series = series[(series >= lower_treshold) & (series <= upper_treshold)]
        domain = DateDomain(focused_series.index.min(), focused_series.index.max())
        return LinearLine(slope=self.slope, intercept=intersect, domain=domain)


class PointLine(Line):
    """
    A point line is defined a :pd.Series: `Series` of points.
    The index is used for the y-axis and the values for the x-axis.

    Args:
        data (pd.Series, array-like, Iterable, dict, or scalar value): A series of points that represent a line.

    Examples
    -------

    >>> line = PointLine(pd.Series([1,2,3,4,5]))
    >>> line * 2
    0     2
    1     4
    2     6
    3     8
    4    10
    dtype: int64
    """

    def __init__(self, data, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not isinstance(data, pd.Series):
            data = pd.Series(data)
        self.data = data

    def __add__(self, other):
        if isinstance(other, Line):
            other = other.make_series()
        return self.__class__(data=self.make_series() + other)

    def __iadd__(self, other):
        self = (self + other)
        return self

    def __sub__(self, other):
        if isinstance(other, Line):
            other = other.make_series()
        return self.__class__(data=self.make_series() - other)

    def __isub__(self, other):
        self = (self - other)
        return self

    def __mul__(self, other):
        if isinstance(other, Line):
            other = other.make_series()
        return self.__class__(data=self.make_series() * other)

    def __imul__(self, other):
        self = (self * other)
        return self

    def __truediv__(self, other):
        if isinstance(other, Line):
            other = other.make_series()
        return self.__class__(data=self.make_series() / other)

    def __idiv__(self, other):
        self = self / other
        return self

    def __repr__(self):
        return repr(self.data)

    def __eq__(self, other):
        if not isinstance(other, PointLine):
            return False
        return self.data.equals(other.data)

    def make_series(self) -> pd.Series:
        return self.data

    def translate_x(self, delta):
        """

        Args:
            delta: Given a delta, shift the line object and return a new line object

        Returns:

        """
        raise NotImplementedError

    def integrate(self):
        """
        https://en.wikipedia.org/wiki/Numerical_integration
        """
        # Temporarily use old cumsum method to mimic old implementation
        integral = self.make_series().cumsum()
        return PointLine(data=integral)

    # TODO: Documentation by Casper van Houten
    def linear_regression(self, data_partition=None):
        """
        Given a set of points, do a linear regression to extrapolate future data
        """
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

    def differentiate(self):
        """
        Calculates difference between previous datapoint on line.

        Returns:
            Instance of PointLine: New Line object (of same type) with difference values per index. NaN on first index.
        """
        difference = self.make_series().diff()
        difference[0] = self.make_series()[0]
        return self.__class__(data=difference)


class TimeseriesLine(PointLine):
    """
    A point line is a collection of datapoints on a shared datetime index.
    """

    def __init__(self, data, domain=None, max_value=None, *args, **kwargs):
        super().__init__(data=data, *args, **kwargs)
        if (len(self.data) == 1) & (domain is not None):
            self.domain = domain
            self.data = pd.Series(data=self.data.iloc[0], index=self.domain.domain)
        elif domain:
            self.domain = domain
        else:
            self.domain = DateDomain(self.data.index[0], self.data.index[-1])
        self.max_value = max_value

    # TODO: Documentation by Casper van Houten
    def make_series(self):
        filled_data = self.data.reindex(self.domain.domain, fill_value=0)
        return filled_data

    # TODO: Documentation by Casper van Houten
    def extrapolate(self, data_partition=None):
        slope, intercept = self.linear_regression(data_partition)
        domain = DateDomain(self.data.index[0], self.data.index[-1])
        return LinearLine(slope=slope,
                          intercept=intercept,
                          domain=domain)

    def integrate(self):
        """
        https://en.wikipedia.org/wiki/Numerical_integration
        """
        # Temporarily use old cumsum method to mimic old implementation
        integral = self.make_series().cumsum()
        return TimeseriesLine(data=integral)

    def append(self, other, skip=0, skip_base=False):
        """

        Args:
            other: Instance of timeseries line of which the values will be added to the end of the current line.
            skip: keyword argument to skip start of index of input line, to be able to append lines that have partially
            overlapping indices.

        Returns:
            A new timeseries line
        """
        if self.domain.end > other.domain.begin:
            raise NotImplementedError("You can only add lines that have a higher index than the line in the object")

        if skip_base:
            series = self.make_series()[:-skip]
            other_series = other.make_series()
        else:
            series = self.make_series()
            other_series = other.make_series()[skip:]

        intersect = series.index.intersection(other_series.index)
        if len(intersect):
            raise ValueError(f"Cannot append Lines that have overlapping indices: {intersect}")

        return TimeseriesLine(series.add(other_series, fill_value=0))

    def translate_x(self, delta=0):
        data = self.data
        data.index = data.index + timedelta(days=delta)
        domain = self.domain.shift(delta)
        return TimeseriesLine(data=data, domain=domain)

    def slice(self, begin=None, end=None):
        if begin is None:
            begin = self.domain.begin
        if end is None:
            end = self.domain.end
        data = self.make_series()[begin:end]
        domain = DateDomain(begin, end)
        return TimeseriesLine(data, domain)

    def linear_regression(self, data_partition=None):
        """
        Given a set of points, do a linear regression to extrapolate future data
        """
        if data_partition:
            shift = int(len(self.domain) * data_partition)
            time_shift = relativedelta(days=shift)
            start = self.data.index[0] + time_shift
            end = self.data.index[-1]
            data = self.data[start:end]
            index = list(range(shift, len(data) + shift))
        else:
            index = list(range(0, len(self.data)))
            data = self.data
        if len(data) >= 2:
            slope, intersect = np.polyfit(index, data, 1)
        else:
            slope = 0
            intersect = 0
        return slope, intersect

    #  this function requires a line based on speed, not distance
    def get_line_aggregate(self, freq='MS', aggregate_type='series', loffset='0', closed='left', index_as_str=False):
        if aggregate_type == 'series':
            series = self.make_normalised_series(maximum=self.max_value, percentage=True)
            aggregate = series.resample(freq, loffset=loffset+freq, closed=closed).sum().cumsum()
            if index_as_str:
                aggregate.index = aggregate.index.format()
        elif aggregate_type == 'value_sum':
            series = self.make_series()
            aggregate = series.resample(freq, loffset=loffset+freq, closed=closed).sum()
            period_for_output = self.period_for_output(freq)
            if period_for_output in series.index:
                aggregate = aggregate[period_for_output]
            else:
                aggregate = 0
        elif aggregate_type == 'value_mean':
            series = self.make_series()
            aggregate = series.resample(freq, loffset=loffset+freq, closed=closed).mean()
            period_for_output = self.period_for_output(freq)
            if period_for_output in series.index:
                aggregate = aggregate[period_for_output]
            else:
                aggregate = 0
        else:
            raise NotImplementedError('No method implemented for aggregate type {}'.format(aggregate_type))

        return aggregate

    def period_for_output(self, freq: str):
        if freq == 'MS':
            period = pd.Timestamp(pd.Timestamp.now().year, pd.Timestamp.now().month, 1) + relativedelta(months=1)
        elif freq == 'W-MON':
            period = pd.to_datetime(pd.Timestamp.now().date() + relativedelta(days=7 - pd.Timestamp.now().weekday()))
        else:
            raise NotImplementedError('There is no output period implemented for this frequency {}'.format(freq))
        return period

    def distance_to_max_value(self):
        return self.max_value - self.integrate().get_most_recent_point()

    def daysleft(self, end=None, slope=None):
        if end:
            if type(end) is str:
                end = pd.to_datetime(end)
            daysleft = (end - self.domain.end).days
        elif slope:
            daysleft = int(self.distance_to_max_value() / slope)
        else:
            daysleft = None
        return daysleft
