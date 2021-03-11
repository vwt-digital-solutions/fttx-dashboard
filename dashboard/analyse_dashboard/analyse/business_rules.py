"""
business_rules.py
=======================

A module that contains the business rules for FttX. Each function in this module is one business rule that operates on
a DataFrame or Series.

"""

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


def make_mask_for_notnan_and_earlier_than_tomorrow_minus_delta(series: pd.Series, time_delta_days: int = 0) -> pd.Series:
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
    return (~series.isna()  # date must be known
            &
            (series <= time_point))  # the date must be before or on the day of the delta.


def geschouwed(df, time_delta_days=0):
    """
    A house is geschouwed when the schouwdatum is not NA and before or on today - time_delta_days.
    This function uses :meth:`make_mask_for_notnan_and_earlier_than_tomorrow_minus_delta`.

    Args:
        df (pd.DataFrame): A dataframe containing a schouwdatum column with dates.
        time_delta_days (int): optional, the number of days before today.

    Returns:
         pd.Series: A series of truth values.
    """
    return make_mask_for_notnan_and_earlier_than_tomorrow_minus_delta(df.schouwdatum, time_delta_days=time_delta_days)


def ordered(df, time_delta_days=0):
    """
    A house is ordered when the toestemming_datum is known.

    Args:
        df: A dataframe containing a toestemming_datum column with dates.
        time_delta_days (int): optional, the number of days before today.

    Returns:
             pd.Series: A series of truth values.
    """
    return make_mask_for_notnan_and_earlier_than_tomorrow_minus_delta(df.toestemming_datum, time_delta_days=time_delta_days)


def actieve_orders_tmobile(df: pd.DataFrame) -> pd.Series:
    """
    Used to calculate the actieve orders for tmobile, based on the business rules: \n
    - Is the df row status not equal to CANCELLED or TO_BE_CANCELLED?
    - Is the df row type equal to AANLEG?

    Args:
        df (pd.DataFrame): A dataframe containing a status and type column.

    Returns:
         pd.Series: A series of truth values.

    """
    return ((~df.status.isin(['CANCELLED', 'TO_BE_CANCELLED']))
            &
            (df.type.isin(['AANLEG', 'Aanleg'])))


def openstaande_orders_tmobile(df: pd.DataFrame, time_delta_days: int = 0,
                               time_window: str = None, order_type: str = None) -> pd.Series:
    """
    Used to calculate the openstaande orders for tmobile, based on the business rules: \n
    -   Is the df row status not equal to CANCELLED, TO_BE_CANCELLED or CLOSED?
    -   Is the df row type equal to AANLEG?
    Additionally, a time window can be set, which adds the following rules: \n
    -   Is the time between today and creation date less than 8 weeks ("on time", 56 days); between 8 & 12 weeks
        ("limited time", 56 & 84 days) or above 12 weeks ("late", 84 days)?

    Args:
        df (pd.DataFrame): A dataframe containing a status, type and creation column, of which creation contains dates.
        time_delta_days (int): optional, the number of days before today.
        time_window (str): A string to set the wanted time window.

    Returns:
         pd.Series: A series of truth values.

    """
    time_point = (pd.Timestamp.today() - pd.Timedelta(days=time_delta_days))

    mask = ((~df.status.isin(['CANCELLED', 'TO_BE_CANCELLED', 'CLOSED']))
            &
            (df.type.isin(['AANLEG', 'Aanleg'])))

    if order_type == 'patch only':
        mask = (mask
                &
                (df.plan_type == 'Zonder klantafspraak'))
    elif order_type == 'hc aanleg':
        mask = (mask
                &
                (df.plan_type != 'Zonder klantafspraak'))

    if time_window == 'on time':
        mask = (mask
                &
                ((time_point - df['creation']).dt.days <= 56))
    elif time_window == 'limited':
        mask = (mask
                &
                (((time_point - df['creation']).dt.days > 56) & ((time_point - df['creation']).dt.days <= 84)))
    elif time_window == 'late':
        mask = (mask
                &
                ((time_point - df['creation']).dt.days > 84))

    return mask


def aangesloten_orders_tmobile(df: pd.DataFrame, time_window: str = None) -> pd.Series:
    """
    Used to calculate the totaal aangesloten orders for tmobile, based on the business rules:

    -   Is the df row status equal to CLOSED?
    -   Is the df row type equal to AANLEG?
    Additionally, a time window can be set, which adds the following rule:

    -   Is the time between opleverdatum and creation date less than 8 weeks ("on time", 56 days)?

    Args:
        df (pd.DataFrame): A dataframe containing a status, type, opleverdatum and creation column, of which the latter
        two contain dates.
        time_window (str): A string to set the wanted time window.

    Returns:
        pd.Series: A series of truth values.

    """

    mask = ((df.status == 'CLOSED')
            &
            (df.type.isin(['AANLEG', 'Aanleg'])))

    if time_window == 'on time':
        mask = (mask
                &
                ((df['opleverdatum'] - df['creation']).dt.days <= 84))

    return mask


def toestemming_bekend(df):
    """
    For a house it is known when permission is given when the `toestemming` column is not NA.

    Args:
        df (pd.DataFrame): A dataframe containing a toestemming column.

    Returns:
         pd.Series: A series of truth values.
    """
    return ~df['toestemming'].isna()


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
    Laswerk AP is not done when `laswerkapgereed` is **not** set to 1.

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
    Laswerk DP is not done when `laswerkdpgereed` is **not** set to 1.

    Args:
        df (pd.DataFrame): A dataframe containing a laswerkdpgereed column containing ones and zeroes.

    Returns:
         pd.Series: A series of truth values.
    """
    return df['laswerkdpgereed'] != '1'


def bis_opgeleverd(df):
    """
    BIS is done when `opleverstatus` is **not** set to 1.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverstatus column with `opleverstatussen`.

    Returns:
         pd.Series: A series of truth values.
    """
    return ~df['opleverstatus'].isin(['0', '90', '99'])


def bis_werkvoorraad(df):
    """
    BIS is werkvoorraad when `opleverstatus` is 0.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverstatus column with `opleverstatussen`.

    Returns:
         pd.Series: A series of truth values.
    """
    return df['opleverstatus'].isin(['0'])


def bis_niet_opgeleverd(df):
    """
    BIS is not done when `opleverstatus` is  set to 0.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverstatus column with `opleverstatussen`.

    Returns:
         pd.Series: A series of truth values.
    """
    return df['opleverstatus'].isin(['0', '90', '99'])


def hc_opgeleverd(df, time_delta_days=0):
    """
    HC (Homes Connected) is done when `opleverstatus` is  set to 2.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverstatus column with `opleverstatussen`.

    Returns:
         pd.Series: A series of truth values.
    """
    mask = make_mask_for_notnan_and_earlier_than_tomorrow_minus_delta(series=df.opleverdatum,
                                                                      time_delta_days=time_delta_days)
    mask = mask & (df.opleverstatus == '2')
    return mask


def hp_opgeleverd(df):
    """
    HP (Homes Passed) is done when `opleverstatus` is **not** set to 2 and `opleverdatum` is not NA.
    Status 2 means a home is connected (see BR hc_opgeleverd).

    Args:
        df (pd.DataFrame): A dataframe containing a opleverstatus column with `opleverstatussen` and a opleverdatum
        column with dates.

    Returns:
         pd.Series: A series of truth values.
    """
    return ((df['opleverstatus'] != '2')
            &
            (~df['opleverdatum'].isna()))


def has_ingeplanned(df):
    """
    HAS is planned when `opleverdatum` is NA and the `hasdatum` is not NA.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverdatum column with dates and a hasdatum
        column with dates.

    Returns:
         pd.Series: A series of truth values.
    """
    return (df['opleverdatum'].isna()
            &
            ~df['hasdatum'].isna())


def has_niet_opgeleverd(df):
    """
    HAS is not completed when `opleverdatum` is NA and the `hasdatum` is NA.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverdatum column with dates and a hasdatum
        column with dates.

    Returns:
         pd.Series: A series of truth values.
    """
    return (df['opleverdatum'].isna()
            &
            # TODO is the hasdatum not the planned date? If so, 'has' can be 'niet opgeleverd' but still be planned.
            df['hasdatum'].isna())


def hpend(df, time_delta_days=0):
    """
    A home is hpend (any kind of completed) when it is :meth:`opgeleverd`.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverdatum column with dates.

    Returns:
         pd.Series: A series of truth values.
    """
    return make_mask_for_notnan_and_earlier_than_tomorrow_minus_delta(df.opleverdatum, time_delta_days=time_delta_days)


def opgeleverd(df, time_delta_days=0):
    """
    A home is completed when `opleverdatum` is not NA and before or on today - time_delta_days.
    This function uses :meth:`make_mask_for_notnan_and_earlier_than_tomorrow_minus_delta`.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverdatum column with dates.
        time_delta_days (int): optional, the number of days before today.

    Returns:
         pd.Series: A series of truth values.
    """
    return make_mask_for_notnan_and_earlier_than_tomorrow_minus_delta(df.opleverdatum, time_delta_days=time_delta_days)


def has_werkvoorraad(df, time_delta_days=0):
    """
    A house is in the HAS werkvoorraad when a permission has been determined and BIS infrastructure is in place.

    This function determines the werkvoorraad HAS by checking each row of a DataFrame for: \n
    -   Does the df row have a schouwdatum AND is the schouwdatum earlier than today?
    -   Does the df row not have a opleverdatum OR is the opleverdatum later than today?
    -   Does the df row have a toestemming_datum?
    -   Is the df row opleverstatus not equal to 0, 90 or 99?

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
            (df.toestemming == 'Ja')
            &
            (~df.opleverstatus.isin(['0', '90', '99']))
    )


def hpend_year(df, year=None):
    """
    A home is hpend (any kind of completed) when it is :meth:`opgeleverd`. This rule only calculates HPend for
    houses that are opgeleverd in the supplied year.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverdatum column with dates.
        year (int): optional, when not supplied the current year is used.

    Returns:
         pd.Series: A series of truth values.
    """
    if not year:
        year = str(pd.Timestamp.now().year)
    start_year = pd.to_datetime(year + '-01-01')
    end_year = pd.to_datetime(year + '-12-31')
    return df.opleverdatum.apply(
        lambda x: (x >= start_year) and (x <= end_year))


def target_tmobile(df):
    """
    This BR determines the target for tmobile by checking each row of a DataFrame for: \n
    -   Does the df row have a creation (date)?
    -   Is the df row status not equal to CANCELLED or TO_BE_CANCELLED?
    -   Is the df row type equal to AANLEG?

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
            (df.type.isin(['AANLEG', 'Aanleg']))
    )
