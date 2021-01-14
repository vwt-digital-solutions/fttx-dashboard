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


# TODO: remove when removing toggle new_structure_overviews
def ordered(df, time_delta_days=0):
    return is_date_set(df.toestemming_datum, time_delta_days=time_delta_days)


def on_time_opgeleverd(df):
    # Used to calculate the homes that have been completed within 8 weeks (56 days)
    # TODO: change toestemming_datum to creation (and check other tmobile business rules)
    return (df['opleverdatum'] - df['toestemming_datum']).dt.days <= 56


def on_time_openstaand(df):
    # Used to calculate the orders for homes that are openstaand within 8 weeks (56 days)
    # TODO: change toestemming_datum to creation
    return (
            ((pd.Timestamp.today() - df['toestemming_datum']).dt.days <= 56)
            &
            (df.opleverdatum.isna() | (df.opleverdatum > pd.Timestamp.today()))
    )


def nog_beperkte_tijd_openstaand(df):
    # Used to calculate the orders for homes that are openstaand between 8 and 12 weeks (56 and 84 days)
    # TODO: change toestemming_datum to creation
    return (
            ((pd.Timestamp.today() - df['toestemming_datum']).dt.days > 56)
            &
            ((pd.Timestamp.today() - df['toestemming_datum']).dt.days <= 84)
            &
            (df.opleverdatum.isna() | (df.opleverdatum > pd.Timestamp.today()))
    )


def te_laat_openstaand(df):
    # Used to calculate the orders for homes that are openstaand above 12 weeks (84 days)
    # TODO: change toestemming_datum to creation
    return (
            ((pd.Timestamp.today() - df['toestemming_datum']).dt.days > 84)
            &
            (df.opleverdatum.isna() | (df.opleverdatum > pd.Timestamp.today()))
    )


def toestemming_bekend(df):
    return ~df['toestemming'].isna()


def toestemming_gegeven(df):
    return ~df['toestemming_datum'].isna()


def laswerk_ap_gereed(df):
    return df['laswerkapgereed'] == '1'


def laswerk_ap_niet_gereed(df):
    return df['laswerkapgereed'] != '1'


def laswerk_dp_gereed(df):
    return df['laswerkdpgereed'] == '1'


def laswerk_dp_niet_gereed(df):
    return df['laswerkdpgereed'] != '1'


# TODO: remove when removing toggle new_structure_overviews
def bis_opgeleverd(df):
    return df['opleverstatus'] != '0'


def bis_opgeleverd_new(df):
    return ~df['opleverstatus'].isin(['0', '90', '99'])


def hpend_opgeleverd(df):
    return ~df['opleverdatum'].isna()


def hpend_opgeleverd_and_ordered(df):
    return (~df['opleverdatum'].isna()) & (~df['ordered'].isna())


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


# TODO: remove when removing toggle new_structure_overviews
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


def has_werkvoorraad_new(df, time_delta_days=0):
    """
    This BR determines the werkvoorraad HAS by checking each row of a DataFrame for:
        Does the df row have a schouwdatum AND is the schouwdatum earlier than today?
        Does the df row not have a opleverdatum OR is the opleverdatum later than today?
        Does the df row have a toestemming_datum?
        Is the df row opleverstatus not equal to 0, 90 or 99?

    :param df: The transformed dataframe
    :param time_delta_days: An optional offset to today's date
    :return: A pd.Series mask
    """
    time_point = (pd.Timestamp.today() - pd.Timedelta(days=time_delta_days))
    return (
            (~df.schouwdatum.isna() & (df.schouwdatum <= time_point))
            &
            (df.opleverdatum.isna() | (df.opleverdatum >= time_point))
            &
            ~df.toestemming_datum.isna()
            &
            ~df.opleverstatus.isin(['0', '90', '99'])
    )


# TODO wat is hpend precies? Gewoon opleverdatum?
def hpend_year(df, year=None):
    if not year:
        year = str(pd.Timestamp.now().year)
    start_year = pd.to_datetime(year + '-01-01')
    end_year = pd.to_datetime(year + '-12-31')
    return df.opleverdatum.apply(
        lambda x: (x >= start_year) and (x <= end_year))


def target_tmobile(df):
    """
    This BR determines the target for tmobile by checking each row of a DataFrame for:
        Does the df row have a creation (date)?
        Is the df row status not equal to CANCELLED or TO_BE_CANCELLED?
        Is the df row type equal to AANLEG?

    :param df: The transformed dataframe
    :return: A pd.Series mask
    """
    return (
            (~df.creation.isna())
            &
            (~df.status.isin(['CANCELLED', 'TO_BE_CANCELLED']))
            &
            (df.type == 'AANLEG')
    )
