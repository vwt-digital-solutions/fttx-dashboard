from datetime import timedelta

import numpy as np
import pandas as pd


class Domain:

    """
    A module to work with ranges of data.
    Ranges can be set up with a start and end number, and can be shifted.

    Examples
    =======

    >>> domain = Domain(1, 5)
    >>> domain.domain
    range(1, 5)

    >>> domain.shift(1).domain
    range(2, 6)

    >>> domain.shift(-1).domain
    range(0, 4)


    """

    def __init__(self, begin, end):
        self.begin = begin
        self.end = end
        self.domain = range(begin, end)

    def shift(self, integer):
        new_begin = self.begin + integer
        new_end = self.end + integer
        return Domain(new_begin, new_end)

    def __len__(self):
        return len(self.domain)

    def __iter__(self):
        for i in range(self.begin, self.end):
            yield i

    def get_range(self):
        """
        Retrieves the domain as np array.
        Returns: NP array with range.

        """
        return np.array(list(range(0, len(self.domain))))

    def get_intersect_index(self, value):
        raise NotImplementedError


class DateDomain(Domain):
    """
    Extension of domain class to specifically work with domains of dateranges.

    >>> domain = DateDomain('2021-01-01', '2021-01-04')
    >>> domain.domain
    DatetimeIndex(['2021-01-01', '2021-01-02', '2021-01-03', '2021-01-04'], dtype='datetime64[ns]', freq='D')
    """

    def __init__(self, begin, end, freq="D"):
        self.begin = pd.to_datetime(begin)
        self.end = pd.to_datetime(end)
        self.domain = pd.date_range(start=begin, end=end, freq=freq)

    def shift(self, days):
        """
        Shifts the range by a given delta

        Args:
            days: amount of days to shift by (into the future)

        Returns: new shifted date domain.

        """
        new_begin = self.begin + timedelta(days=days)
        new_end = self.end + timedelta(days=days)
        return DateDomain(begin=new_begin, end=new_end)

    def slice_domain(self, start_offset, stop_offset=0):
        """
        Slices a date domain from a certain date, and returns the sliced domain.

        Args:
            start_offset: First day that appears in the new date range
            stop_offset: Last date to appear in the daterange. (optional)

        Returns: sliced date domain.

        """
        return DateDomain(begin=self.begin + start_offset, end=self.end + stop_offset)

    def get_intersect_index(self, value):
        """
        Retrieves the index of a certain date in the range.
        Args:
            value: date to retrieve the index of.

        Returns: Index of the day.
        """

        return (value - self.begin).days


class DateDomainRange(DateDomain):
    def __init__(self, begin, n_days):
        self.begin = pd.to_datetime(begin)
        self.end = pd.to_datetime(begin) + timedelta(days=n_days)
        self.domain = pd.date_range(start=begin, end=self.end, freq="D")
