import datetime

import numpy as np
import pandas as pd
import pytest
from tests.business_rules_validators import (validate_bis_opgeleverd,
                                             validate_laswerk_ap_gereed,
                                             validate_laswerk_dp_gereed)

import business_rules as br
import config
from Analyse.KPNDFN import KPNLocalETL
from hypothesis import given, settings
from hypothesis.extra.pandas import column, data_frames, series
from hypothesis.strategies import builds, dates, integers, one_of, sampled_from

settings.register_profile(
    "my_profile",
    max_examples=200,
    deadline=60
    * 1000,  # Allow 1 min per example (deadline is specified in milliseconds)
)


def nat_strategy():
    return np.datetime64("NaT")


nats = builds(nat_strategy)

opgeleverdatum_st = data_frames(
    [
        column(
            "opleverdatum",
            dtype="datetime64[ns]",
            elements=one_of(
                dates(
                    min_value=datetime.date(2019, 1, 1), max_value=datetime.date.today()
                ),
                nats,
            ),
        )
    ]
)

opleverstatus_st = data_frames(
    [
        column(
            "opleverstatus", dtype="object", elements=sampled_from(br.opleverstatussen)
        )
    ]
)

date_series_st = series(
    dates(min_value=datetime.date(2019, 1, 1), max_value=datetime.date.today()),
    dtype="datetime64[ns]",
)


@given(date_series_st, integers(min_value=-10, max_value=10))
def test_is_date_set(test_series: pd.Series, time_delta_days: int):
    opgeleverd = br.is_date_set(test_series, time_delta_days)
    time_point = datetime.date.today() - datetime.timedelta(days=time_delta_days)
    assert len(test_series) == len(opgeleverd)
    if not test_series.empty:
        assert ((~test_series.isna() & (test_series <= time_point)) == opgeleverd).all()


@given(opleverstatus_st)
def test_bis_opgeleverd(df):
    validate_bis_opgeleverd(df)


@pytest.fixture(scope="session")
def real_data_df():
    client_name = "kpn"
    kpn = KPNLocalETL(client=client_name, config=config.client_config[client_name])
    kpn.extract()
    kpn.transform()
    return kpn.transformed_data.df


@pytest.mark.slow
class TestWithRealData:
    def test_bis_opgeleverd(self, real_data_df):
        validate_bis_opgeleverd(real_data_df)

    def test_bis_laswerk_dp_gereed(self, real_data_df: pd.DataFrame):
        validate_laswerk_dp_gereed(real_data_df)

    def test_bis_laswerk_ap_gereed(self, real_data_df: pd.DataFrame):
        validate_laswerk_ap_gereed(real_data_df)
