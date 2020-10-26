import pandas as pd
from hypothesis import given, settings, example
from hypothesis.strategies import dates, builds, one_of, integers
from hypothesis.extra.pandas import column, data_frames
import datetime
import numpy as np

import business_rules as br

settings.register_profile(
    "my_profile",
    max_examples=200,
    deadline=60 * 1000,  # Allow 1 min per example (deadline is specified in milliseconds)
)


def nat_strategy():
    return np.datetime64("NaT")


nats = builds(nat_strategy)

opgeleverd_st = data_frames([
    column(
        'opleverdatum',
        dtype='datetime64[ns]',
        elements=one_of(
            dates(min_value=datetime.date(2019, 1, 1), max_value=datetime.date.today()),
            nats
        )
    )
]
)


@given(
    opgeleverd_st
)
def test_opgeleverd(df):
    opgeleverd = br.opgeleverd(df)
    assert df.empty == opgeleverd.empty
    assert len(df) == len(opgeleverd)
    if not df.empty:
        assert (~df.opleverdatum.isna() == opgeleverd).all()


@given(opgeleverd_st,
       integers(min_value=-10, max_value=10))
@example(pd.DataFrame(
    [datetime.date.today() - datetime.timedelta(days=x) for x in range(10)],
    columns=["opleverdatum"],
    dtype='datetime64[ns]'
), 2)
def test_opgeleverd_with_date(df, time_delta_days):
    opgeleverd = br.opgeleverd(df, time_delta_days)
    time_point = datetime.date.today() - datetime.timedelta(days=time_delta_days)
    assert df.empty == opgeleverd.empty
    if not df.empty:
        df['opgeleverd'] = opgeleverd
        assert (
                (~df.opleverdatum.isna() & (df.opleverdatum <= time_point)) == df.opgeleverd
        ).all()
