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
"""
opleverstatussen is a list that contains all possible opleverstatussen.
"""


def is_date_set(series: pd.Series, time_delta_days: int = 0) -> pd.Series:
    """
    A function that creates a mask for the supplied series that lists which dates are before today's date minus the
    time_delta_days.

    By default it checks if it is set for today or earlier.
    A date must be known (not isna), and it must be before or on the checked date (today minus time_delta_days)

    Args:
        series (pd.Series): A series of dates to check.
        time_delta_days (int): optional, the number of days before today.

    Returns:
         pd.Series: A series of truth values.
    """
    time_point: pd.Timestamp = (pd.Timestamp.today() - pd.Timedelta(days=time_delta_days))
    return (
            ~series.isna() &  # date must be known
            (
                    series <= time_point  # the date must be before or on the day of the delta.
            )
    )


def geschouwed(df, time_delta_days=0):
    """
    A house is geschouwed when the schouwdatum is not NA and before or on today - time_delta_days.
    This function uses :meth:`is_date_set`.

    Args:
        df (pd.DataFrame): A dataframe containing a schouwdatum column with dates.
        time_delta_days (int): optional, the number of days before today.

    Returns:
         pd.Series: A series of truth values.
    """
    return is_date_set(df.schouwdatum, time_delta_days=time_delta_days)


# TODO: remove when removing toggle new_structure_overviews
def ordered(df, time_delta_days=0):
    return is_date_set(df.toestemming_datum, time_delta_days=time_delta_days)


def on_time_opgeleverd(df):
    """
    Used to calculate the homes that have been completed within 8 weeks (56 days) after providing permission.

    Args:
        df (pd.DataFrame): A dataframe containing an opleverdatum column and toestemming_datum column, both containing
        dates.

    Returns:
         pd.Series: A series of truth values.

    """
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
    """
    For a house it is known when permission is given when the `toestemming` column is not NA.

    Args:
        df (pd.DataFrame): A dataframe containing a toestemming column.

    Returns:
         pd.Series: A series of truth values.
    """
    return ~df['toestemming'].isna()


# TODO: Tjeerd Pols, remove this function.
def toestemming_gegeven(df):
    return ~df['toestemming_datum'].isna()


def laswerk_ap_gereed(df):
    """
    Laswerk AP is done when `laswerkapgereed` is set to 1.

    Args:
        df (pd.DataFrame): A dataframe containing a laswerkapgereed column containing ones and zeroes.

    Returns:
         pd.Series: A series of truth values.
    """
    return df['laswerkapgereed'] == '1'


def laswerk_ap_niet_gereed(df):
    """
    Laswerk AP is done when `laswerkapgereed` is **not** set to 1.

    Args:
        df (pd.DataFrame): A dataframe containing a laswerkapgereed column containing ones and zeroes.

    Returns:
         pd.Series: A series of truth values.
    """
    return df['laswerkapgereed'] != '1'


def laswerk_dp_gereed(df):
    """
    Laswerk DP is done when `laswerkdpgereed` is set to 1.

    Args:
        df (pd.DataFrame): A dataframe containing a laswerkdpgereed column containing ones and zeroes.

    Returns:
         pd.Series: A series of truth values.
    """
    return df['laswerkdpgereed'] == '1'


def laswerk_dp_niet_gereed(df):
    """
    Laswerk DP is done when `laswerkdpgereed` is **not** set to 1.

    Args:
        df (pd.DataFrame): A dataframe containing a laswerkdpgereed column containing ones and zeroes.

    Returns:
         pd.Series: A series of truth values.
    """
    return df['laswerkdpgereed'] != '1'


# TODO: remove when removing toggle new_structure_overviews
def bis_opgeleverd(df):
    """
    BIS is done when `opleverstatus` is **not** set to 1.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverstatus column with `opleverstatussen`.

    Returns:
         pd.Series: A series of truth values.
    """
    return df['opleverstatus'] != '0'


def bis_niet_opgeleverd(df):
    """
    BIS is not done when `opleverstatus` is  set to 0.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverstatus column with `opleverstatussen`.

    Returns:
         pd.Series: A series of truth values.
    """
    return df['opleverstatus'] == '0'


# Todo: Andre van Turnhout: Document/remove this function. It should not be named something_new.
def bis_opgeleverd_new(df):
    return ~df['opleverstatus'].isin(['0', '90', '99'])


# TODO: Andre van Turnhout. You should probably have used the hpend() function. If so: refactor the code that uses this
#  function, otherwise document this function
def hpend_opgeleverd(df):
    return ~df['opleverdatum'].isna()


# TODO: Tjeerd Pols Document this function. Reuse other business rules when applicable.
#  Here you need to use the opgeleverd and orderd rules.
def hpend_opgeleverd_and_ordered(df):
    return (~df['opleverdatum'].isna()) & (~df['ordered'].isna())


def hc_opgeleverd(df):
    """
    HC (Homes Connected) is done when `opleverstatus` is  set to 2.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverstatus column with `opleverstatussen`.

    Returns:
         pd.Series: A series of truth values.
    """
    return df['opleverstatus'] == '2'


def hp_opgeleverd(df):
    """
    HP (Homes Passed) is done when `opleverstatus` is **not** set to 2 and `opleverdatum` is not NA.
    Status 2: a home is connected.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverstatus column with `opleverstatussen` and a opleverdatum
        column with dates.

    Returns:
         pd.Series: A series of truth values.
    """
    return (
            (df['opleverstatus'] != '2') &
            (~df['opleverdatum'].isna())
    )


def has_ingeplanned(df):
    """
    HAS is planned when `opleverdatum` is NA and the `hasdatum` is not NA.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverdatum column with dates and a hasdatum
        column with dates.

    Returns:
         pd.Series: A series of truth values.
    """
    return (
            df['opleverdatum'].isna() &
            ~df['hasdatum'].isna()
    )


def has_niet_opgeleverd(df):
    """
    HAS is not completed when `opleverdatum` is NA and the `hasdatum` is NA.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverdatum column with dates and a hasdatum
        column with dates.

    Returns:
         pd.Series: A series of truth values.
    """
    return (
            df['opleverdatum'].isna() &
            # TODO is the hasdatum not the planned date? If so, 'has' can be 'niet opgeleverd' but still be planned.
            df['hasdatum'].isna()
    )


def hpend(df, time_delta_days=0):
    """
    A home is hpend (any kind of completed) when it is :meth:`opgeleverd`.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverdatum column with dates.

    Returns:
         pd.Series: A series of truth values.
    """
    return opgeleverd(df, time_delta_days)


def opgeleverd(df, time_delta_days=0):
    """
    A home is completed when `opleverdatum` is not NA and before or on today - time_delta_days.
    This function uses :meth:`is_date_set`.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverdatum column with dates.
        time_delta_days (int): optional, the number of days before today.

    Returns:
         pd.Series: A series of truth values.
    """
    return is_date_set(df.opleverdatum, time_delta_days=time_delta_days)


def has_werkvoorraad(df, time_delta_days=0):
    """
    This BR determines the werkvoorraad HAS by checking each row of a DataFrame for:

    - Does the df row have a schouwdatum AND is the schouwdatum earlier than today?
    - Does the df row not have a opleverdatum OR is the opleverdatum later than today?
    - Does the df row have a toestemming_datum?
    - Is the df row opleverstatus not equal to 0, 90 or 99?

    Args:
        df (pd.DataFrame): A  dataframe containing the following columns: [schouwdatum, opleverdatum,
         toestemming_datum, opleverstatus]
        time_delta_days (int): optional, the number of days before today.

    Returns:
        pd.Series: A series of truth values.
    """
    time_point = (pd.Timestamp.today() - pd.Timedelta(days=time_delta_days))
    return (
            (~df.schouwdatum.isna() & (df.schouwdatum <= time_point))
            &
            (df.opleverdatum.isna() | (df.opleverdatum > time_point))
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

    - Does the df row have a creation (date)?
    - Is the df row status not equal to CANCELLED or TO_BE_CANCELLED?
    - Is the df row type equal to AANLEG?

    Args:
        df (pd.DataFrame): The transformed dataframe

    Returns:
        pd.Series: A series of truth values.
    """
    return (
            (~df.creation.isna())
            &
            (~df.status.isin(['CANCELLED', 'TO_BE_CANCELLED']))
            &
            (df.type == 'AANLEG')
    )
