from datetime import timedelta

import numpy as np
import pandas as pd


# TODO: Documentation by Casper van Houten
# TODO: add examples
class Domain:
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

    # TODO: Documentation by Casper van Houten
    def get_range(self):
        return np.array(list(range(0, len(self.domain))))

    # TODO: Documentation by Casper van Houten
    def get_intersect_index(self, value):
        raise NotImplementedError


# TODO: Documentation by Casper van Houten
class DateDomain(Domain):
    def __init__(self, begin, end):
        print(f'making domain between {begin}, {end}')
        self.begin = pd.to_datetime(begin)
        self.end = pd.to_datetime(end)
        self.domain = pd.date_range(start=begin,
                                    end=end,
                                    freq='D'
                                    )

    # TODO: Documentation by Casper van Houten
    def shift(self, days):
        new_begin = self.begin + timedelta(days=days)
        new_end = self.end + timedelta(days=days)
        return DateDomain(begin=new_begin,
                          end=new_end
                          )

    # TODO: Documentation by Casper van Houten
    def slice_domain(self, start_offset, stop_offset=0):
        return DateDomain(begin=self.begin + start_offset,
                          end=self.end + stop_offset)

    # TODO: Documentation by Casper van Houten
    def get_intersect_index(self, value):
        return (value - self.begin).days


class DateDomainRange(DateDomain):
    def __init__(self, begin, n_days):
        self.begin = pd.to_datetime(begin)
        self.end = pd.to_datetime(begin) + timedelta(days=n_days)
        self.domain = pd.date_range(start=begin,
                                    end=self.end,
                                    freq='D'
                                    )
