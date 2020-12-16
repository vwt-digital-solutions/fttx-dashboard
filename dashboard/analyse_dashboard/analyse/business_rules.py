import pandas as pd

opleverstatussen = [
    "0",
    "1",
    "2",
    "5",
    "6",
    "7",
    "8",
    "9",
    "10",
    "11",
    "13",
    "14",
    "30",
    "31",
    "33",
    "34",
    "35",
    "50",
    "90",
    "91"
]


def is_date_set(series: pd.Series, time_delta_days: int = 0) -> pd.Series:
    """
    To determine if a date is set.

    This function determines if a dates is set at a particular date. By default it checks if it is set for today or
    earlier. A date must be known (not isna), and it must be before or on the checked date (today minus time_delta_days)
    :param series:
    :param time_delta_days:
    :return:
    """
    time_point: pd.Timestamp = (pd.Timestamp.today() - pd.Timedelta(days=time_delta_days))
    return (
            ~series.isna() &  # date must be known
            (
                    series <= time_point  # the date must be before or on the day of the delta.
            )
    )


def geschouwed(df, time_delta_days=0):
    return is_date_set(df.schouwdatum, time_delta_days=time_delta_days)


def ordered(df, time_delta_days=0):
    return is_date_set(df.toestemming_datum, time_delta_days=time_delta_days)


def toestemming_bekend(df):
    return ~df['toestemming'].isna()


def laswerk_ap_gereed(df):
    return df['laswerkapgereed'] == '1'


def laswerk_ap_niet_gereed(df):
    return df['laswerkapgereed'] != '1'


def laswerk_dp_gereed(df):
    return df['laswerkdpgereed'] == '1'


def laswerk_dp_niet_gereed(df):
    return df['laswerkdpgereed'] != '1'


def bis_opgeleverd(df):
    return df['opleverstatus'] != '0'


def bis_opgeleverd_new(df):
    return ~df['opleverstatus'].isin(['0', '90', '99'])


def hpend_opgeleverd(df):
    return ~df['opleverdatum'].isna()


def bis_niet_opgeleverd(df):
    return df['opleverstatus'] == '0'


def hc_opgeleverd(df):
    return df['opleverstatus'] == '2'


def hp_opgeleverd(df):
    return (
            (df['opleverstatus'] != '2') &
            (~df['opleverdatum'].isna())
    )


def has_ingeplanned(df):
    return (
            df['opleverdatum'].isna() &
            ~df['hasdatum'].isna()
    )


def has_niet_opgeleverd(df):
    return (
            df['opleverdatum'].isna() &
            # TODO is the hasdatum not the planned date? If so, 'has' can be 'niet opgeleverd' but still be planned.
            df['hasdatum'].isna()
    )


def hpend(df, time_delta_days=0):
    return opgeleverd(df, time_delta_days)


def opgeleverd(df, time_delta_days=0):
    return is_date_set(df.opleverdatum, time_delta_days=time_delta_days)


def has_werkvoorraad(schouw_df, time_delta_days=0):
    time_point = (pd.Timestamp.today() - pd.Timedelta(days=time_delta_days))
    return (
            (
                (
                        ~schouw_df.schouwdatum.isna() &
                        (
                                schouw_df.schouwdatum <= time_point
                        )
                )
            ) &
            (
                    schouw_df.opleverdatum.isna() |
                    (
                            schouw_df.opleverdatum >= time_point
                    )
            ) &
            (
                ~schouw_df.toestemming_datum.isna()
            ) &
            (
                    schouw_df.opleverstatus != '0'
            )
    )


# TODO wat is hpend precies? Gewoon opleverdatum?
def hpend_year(df, year=None):
    if not year:
        year = str(pd.Timestamp.now().year)
    start_year = pd.to_datetime(year + '-01-01')
    end_year = pd.to_datetime(year + '-12-31')
    return df.opleverdatum.apply(
        lambda x: (x >= start_year) and (x <= end_year))
