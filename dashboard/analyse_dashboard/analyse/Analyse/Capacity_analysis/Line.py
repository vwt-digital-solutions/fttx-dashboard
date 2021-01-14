"""
Line.py
=====================
A module to work and calculate with Lines
"""

import base64
from io import BytesIO

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

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

    def make_normalised_series(self, maximum=None) -> pd.Series:
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
        return series / maximum

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

    def translate_x(self, delta):
        translated_intersect = self.intercept - delta * self.slope
        new_domain = self.domain.shift(delta)
        translated_line = LinearLine(slope=self.slope,
                                     intercept=translated_intersect,
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
            intersect = self.intercept
        series = self.make_series()
        focused_series = series[(series >= lower_treshold) & (series <= upper_treshold)]
        domain = DateDomain(focused_series.index.min(), focused_series.index.max())
        return LinearLine(slope=self.slope, intercept=intersect, domain=domain)


class PointLine(Line):
    """
    A point line is defined a :pd.Series: `Series` of points. The index is used for the y-axis and the values for the x-axis.

    Args:
        data:
    """

    def __init__(self, data: pd.Series, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data = data

    def __truediv__(self, other):
        if isinstance(other, FunctionLine):
            other = PointLine(other.make_series())

        if isinstance(other, PointLine):
            return self.__class__(data=self.make_series() / other.make_series())
        else:
            try:
                return self.__class__(data=self.make_series().divide(other))
            except TypeError as e:
                raise e

    def __idiv__(self, other):
        self = self.__truediv__(other)

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


class TimeseriesLine(PointLine):
    """
    A point line is a collection of datapoints on a shared datetime index.
    """

    def __init__(self, data, domain=None, *args, **kwargs):
        super().__init__(data=data, *args, **kwargs)
        if domain:
            self.domain = domain
        else:
            self.domain = DateDomain(data.index[0], data.index[-1])

    def make_series(self):
        filled_data = self.data.reindex(self.domain.domain, fill_value=0)
        return filled_data

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
