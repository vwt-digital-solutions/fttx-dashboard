"""
business_rules.py
=======================

A module that contains the business rules for FttX. Each function in this module is one business rule that operates on
a DataFrame or Series.

"""

import pandas as pd


def notna_and_earlier_than_tomorrow_minus_delta(
    series: pd.Series, time_delta_days: int = 0
) -> pd.Series:
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
    time_point: pd.Timestamp = pd.Timestamp.today() - pd.Timedelta(days=time_delta_days)
    return series.notna() & (series <= time_point)


def geschouwd(df: pd.DataFrame) -> pd.Series:
    """
    A house is geschouwed when the schouwdatum is not NA and before tomorrow - time_delta_days.
    This function uses :meth:`notna_and_earlier_than_tomorrow_minus_delta`.

    Args:
        df (pd.DataFrame): A dataframe containing a schouwdatum column with dates.

    Returns:
         pd.Series: A series of truth values.
    """
    return notna_and_earlier_than_tomorrow_minus_delta(df.schouwdatum)


def open_order_tmobile(df: pd.DataFrame) -> pd.Series:
    """
        Used to calculate the openstaande orders for tmobile, based on the business rules: \n
    -   Is the df row status not equal to CANCELLED, TO_BE_CANCELLED or CLOSED?
    -   Is the df row type equal to AANLEG?
    Args:
         df (pd.DataFrame): A dataframe containing a status, type and creation column, of which creation contains dates.

    Returns: pd.Series: A series of truth values.

    """
    return (~df.status.isin(["CANCELLED", "TO_BE_CANCELLED", "CLOSED"])) & (
        df.type.isin(["AANLEG", "Aanleg"])
    )


def hc_patch_only_tmobile(df: pd.DataFrame, time_window=None):
    mask = open_order_tmobile(df) & (df.plan_type == "Zonder klantafspraak")
    if time_window:
        mask = add_time_window(mask, time_window, ds_dates=df["creation"])
    return mask


def hc_aanleg_tmobile(df: pd.DataFrame, time_window=None):
    mask = open_order_tmobile(df) & (df.plan_type != "Zonder klantafspraak")
    if time_window:
        mask = add_time_window(mask, time_window, ds_dates=df["creation"])
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

    mask = (df.status == "CLOSED") & (df.type.isin(["AANLEG", "Aanleg"]))

    if time_window:
        mask = add_time_window(
            mask, time_window, ds_dates=df.creation, time_point=df.opleverdatum
        )

    return mask


def add_time_window(mask, time_window, ds_dates, time_point=pd.Timestamp.today()):
    if time_window == "on time":
        mask = mask & ((time_point - ds_dates).dt.days <= 56)
    elif time_window == "limited":
        mask = mask & (
            ((time_point - ds_dates).dt.days > 56)
            & ((time_point - ds_dates).dt.days <= 84)
        )
    elif time_window == "late":
        mask = mask & ((time_point - ds_dates).dt.days > 84)
    elif time_window == "ratio":
        mask = mask & ((time_point - ds_dates).dt.days <= 84)
    return mask


def toestemming(df):
    """
    For a house it is known when permission is given when the `toestemming` column is not NA.

    Args:
        df (pd.DataFrame): A dataframe containing a toestemming column.

    Returns:
         pd.Series: A series of truth values.
    """
    return df["toestemming"] == "Ja"


def laswerk_ap_gereed(df):
    """
    Laswerk AP is done when `laswerkapgereed` is set to 1.

    Args:
        df (pd.DataFrame): A dataframe containing a laswerkapgereed column containing ones and zeroes.

    Returns:
         pd.Series: A series of truth values.
    """
    return df["laswerkapgereed"] == "1"


def laswerk_dp_gereed(df):
    """
    Laswerk DP is done when `laswerkdpgereed` is set to 1.

    Args:
        df (pd.DataFrame): A dataframe containing a laswerkdpgereed column containing ones and zeroes.

    Returns:
         pd.Series: A series of truth values.
    """
    return df["laswerkdpgereed"] == "1"


def bis_opgeleverd(df):
    """
    BIS is done when `opleverstatus` is **not** set to 1.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverstatus column with `opleverstatussen`.

    Returns:
         pd.Series: A series of truth values.
    """
    return ~df["opleverstatus"].isin(["0", "90", "91", "99"])


def hc_opgeleverd(df):
    """
    HC (Homes Connected) is done when `opleverstatus` is  set to 2.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverstatus column with `opleverstatussen`.

    Returns:
         pd.Series: A series of truth values.
    """
    mask = notna_and_earlier_than_tomorrow_minus_delta(df.opleverdatum)
    mask = mask & (df.opleverstatus == "2")
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
    mask = notna_and_earlier_than_tomorrow_minus_delta(df.opleverdatum)
    mask = mask & (df["opleverstatus"] != "2")
    return mask


def niet_opgeleverd(df):
    return ~notna_and_earlier_than_tomorrow_minus_delta(df.opleverdatum)


def niet_opgeleverd_wel_has_gepland(df):
    """
    HAS is planned when `opleverdatum` is NA and the `hasdatum` is not NA.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverdatum column with dates and a hasdatum
        column with dates.

    Returns:
         pd.Series: A series of truth values.
    """
    return niet_opgeleverd(df) & (df["hasdatum"].notna())


def niet_opgeleverd_niet_has_gepland(df):
    """
    HAS is not completed when `opleverdatum` is NA and the `hasdatum` is NA.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverdatum column with dates and a hasdatum
        column with dates.

    Returns:
         pd.Series: A series of truth values.
    """
    return niet_opgeleverd(df) & (df["hasdatum"].isna())


def has_gepland(df):
    """
    HAS is gepland when `hasdatum` is not NA

    Args:
        df (pd.DataFrame): A dataframe containing a hasdatum column with dates.

    Returns:
         pd.Series: A series of truth values.
    """
    return df["hasdatum"].notna()


def hpend(df):
    """
    A home is hpend (any kind of completed) when it is :meth:`opgeleverd`.

    Args:
        df (pd.DataFrame): A dataframe containing a opleverdatum column with dates.

    Returns:
         pd.Series: A series of truth values.
    """
    mask = hc_opgeleverd(df) | hp_opgeleverd(df)
    return mask


def has_werkvoorraad(df):
    """
    A house is in the HAS werkvoorraad when a permission has been determined and BIS infrastructure is in place.

    This function determines the werkvoorraad HAS by checking each row of a DataFrame for: \n
    -   Does the df row have a schouwdatum AND is the schouwdatum earlier than today?
    -   Does the df row not have a opleverdatum OR is the opleverdatum later than today?
    -   Does the df row have toestemming?
    -   Does the df row have bis_opgeleverd?

    Args:
        df (pd.DataFrame): A  dataframe containing the following columns: [schouwdatum, opleverdatum,
         toestemming, opleverstatus]

    Returns:
        pd.Series: A series of truth values.
    """
    return bis_opgeleverd(df) & geschouwd(df) & toestemming(df) & niet_opgeleverd(df)


def leverbetrouwbaar(df: pd.DataFrame):
    """
    This BR determines if the house is delivered in time (leverbetrouwbaar), thus:
    -   The opleverdatum is not empty
    -   The opleverdatum == hasdatum
    -   The hasdatum has not changed within 3 days of the opleverdatum
    Args:
        df (pd.DataFrame): The transformed dataframe

    Returns:
        pd.Series: A series of truth values
    """

    mask = (
        (df.opleverdatum == df.hasdatum)
        & (df.hasdatum_change_date < (df.opleverdatum - pd.Timedelta(days=2)))
        & hpend(df)
    )

    return mask


def mask_werkvoorraad_activatie_lb_FC(df: pd.DataFrame):
    """
    Dataframe mask returning a column if object is in werkvoorraad for LB.
    Args:
        df: Dataframe of combined FC and Bouwportaal data.

    Returns: boolean mask of objects in LB werkvoorraad or not.

    """
    return (df.opleverstatus == "16") & (df.opleverdatum.isna())


def mask_werkvoorraad_activatie_hb_FC(df: pd.DataFrame):
    """
    Dataframe mask returning a column if object is in werkvoorraad for HB.
    Args:
        df: Dataframe of combined FC and Bouwportaal data.

    Returns: boolean mask of objects in HB werkvoorraad or not.

    """
    return ((df.opleverstatus == "2") | (df.opleverstatus == "32")) & (
        df.opleverdatum.isna()
    )


def mask_werkvoorraad_activatie_lb_assigned(df: pd.DataFrame):
    """
    Dataframe mask returning a column if object is in werkvoorraad assigned for LB.
    Args:
        df: Dataframe of combined FC and Bouwportaal data.

    Returns: boolean mask of objects in LB werkvoorraad or not.

    """
    return (
        (df.ordertype == "CONSTRUCT")
        & (df.orderdatum.notna())
        & (df.soort_bouw == "Laag")
    )


def mask_werkvoorraad_activatie_hb_assigned(df: pd.DataFrame):
    """
    Dataframe mask returning a column if object is in werkvoorraad assigned for HB.
    Args:
        df: Dataframe of combined FC and Bouwportaal data.

    Returns: boolean mask of objects in HB werkvoorraad or not.

    """
    return (
        (df.ordertype == "CONSTRUCT")
        & (df.orderdatum.notna())
        & (df.soort_bouw != "Laag")
    )


def mask_aanvragen_activatie_lb(df: pd.DataFrame):
    """
    Dataframe mask returning a column if object is in aanvraagde activatie voor laagbouw.
    Args:
        df: Dataframe of combined FC and Bouwportaal data.

    Returns: boolean mask of objects.

    """
    return (df.ordertype == "CONSTRUCT") & (df.soort_bouw == "Laag")


def mask_aanvragen_activatie_hb(df: pd.DataFrame):
    """
    Dataframe mask returning a column if object is in aanvraagde activatie voor hoogbouw.
    Args:
        df: Dataframe of combined FC and Bouwportaal data.

    Returns: boolean mask of objects.

    """
    return (df.ordertype == "CONSTRUCT") & (df.soort_bouw != "Laag")


def mask_openstaande_aanvragen_ndagen_te_laat(df: pd.DataFrame, ndays=2):
    """
    Dataframe mask returning a column if object is openstaande aanvraag is ndays
    too late according to planning.
    Args:
        df: Dataframe of combined FC and Bouwportaal data.

    Returns: boolean mask of objects.

    """
    return (~df.order_status.isin(["CLOSED", "CANCELLED", "TO_BE_CANCELLED"])) & (
        df.plandatum < pd.Timestamp.now() - pd.Timedelta(days=ndays)
    )
