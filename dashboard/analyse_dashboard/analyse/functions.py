"""
functions.py
================

A big collection of functions used in the analysis.
"""


from collections import defaultdict

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from google.cloud import firestore, secretmanager
from sqlalchemy import create_engine

import config

colors = config.colors_vwt


def create_project_filter(df: pd.DataFrame):
    """
    Creates a filter based on the projects in the dataframe.
    Args:
        df (pd.DataFrame): A dataframe containing a categorical projects column.

    Returns:
        list[dict]: A list of dictionaries with the projects with the following shape
        {'label': project, 'value': project}
    """
    filters = [{"label": x, "value": x} for x in df.project.cat.categories]
    record = dict(filters=filters)
    return record


def round_(data):
    if isinstance(data, float):
        if (data > -1) & (data < 1):
            data = int(data * 10 ** 2) / 10 ** 2
        elif pd.isnull(data):
            data = 0
        else:
            data = int(data)
    elif isinstance(data, pd.Series):
        data = data.fillna(0)
        if (data.max() >= -1) & (data.max() <= 1):
            data = (data * 10 ** 2).astype(int) / 10 ** 2
        else:
            data = data.astype(int)

    return data


def get_map_bnumber_vs_project_from_sql():
    """
    This method extracts the bnumber, project name mapping table from the sql database.

    Returns:
            pd.DataFrame: a dataframe with bnumbers as keys and project names as values

    """
    sql_engine = get_database_engine()
    df = pd.read_sql("fc_baan_project_nr_name_map", sql_engine)
    ds_mapping = (
        df[["fiberconnect_code", "project_naam"]]
        .dropna()
        .set_index("fiberconnect_code")
    )
    ds_mapping.index = ds_mapping.index.astype(int).astype(str)
    ds_mapping = ds_mapping[~ds_mapping.duplicated()].rename(
        columns={"project_naam": "project"}
    )
    ds_mapping.index.name = "bnummer"
    return ds_mapping


def set_date_update(client=None):
    """
    This functions sets the date for the last time the analysis function has run correctly
    Since we have disinct analysis functions for each client, the update date is set for a
    specific client.

    Args:
        client: client name

    Returns: timestamp store in a document for the last correct run of the analysis

    """
    id_ = f"update_date_{client}" if client else "update_date"
    record = dict(id=id_, date=pd.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
    firestore.Client().collection("Graphs").document(record["id"]).set(record)


def error_check_FCBC(df: pd.DataFrame):
    """This function determines how many houses in the dataframe fall
    in a specific error category (named business rules). The type of errors
    are documented at the BCExport pdf supplied by KPN. All the errors are related to
    faulty logging in FiberConnect which leads to refusal of import at BCexport.

    Args:
        df (pd.DataFrame): dataframe with information on houses from FiberConnect.

    Returns:
        n_err: Dictionary with total number of errors per project.
        errors_FC_BC: Dictionary of list of sleutels per error type per project.
    """
    business_rules = {}

    no_errors_series = pd.Series([False]).repeat(len(df)).values

    business_rules["101"] = (
        df.kabelid.isna()
        & ~df.opleverdatum.isna()
        & (df.postcode.isna() | df.huisnummer.isna())
    )
    business_rules["102"] = df.plandatum.isna()
    business_rules["103"] = df.opleverdatum.isna() & df.opleverstatus.isin(
        ["2", "10", "90", "91", "96", "97", "98", "99"]
    )
    business_rules["104"] = df.opleverstatus.isna()
    # business_rules['114'] = (df.toestemming.isna())
    business_rules["115"] = business_rules[
        "118"
    ] = df.soort_bouw.isna()  # soort_bouw hoort bij?
    business_rules["116"] = df.ftu_type.isna()
    business_rules["117"] = df["toelichting_status"].isna() & df.opleverstatus.isin(
        ["4", "12"]
    )
    business_rules["119"] = df["toelichting_status"].isna() & df.redenna.isin(
        ["R8", "R9", "R17"]
    )

    business_rules["120"] = no_errors_series  # doorvoerafhankelijk niet aanwezig
    business_rules["121"] = (df.postcode.isna() & ~df.huisnummer.isna()) | (
        ~df.postcode.isna() & df.huisnummer.isna()
    )
    business_rules["122"] = ~(
        (
            df.kast.isna()
            & df.kastrij.isna()
            & df.odfpos.isna()
            & df.catvpos.isna()
            & df.odf.isna()
        )
        | (
            ~df.kast.isna()
            & ~df.kastrij.isna()
            & ~df.odfpos.isna()
            & ~df.catvpos.isna()
            & ~df.areapop.isna()
            & ~df.odf.isna()
        )
    )  # kloppen deze velden?  (kast, kastrij, odfpos)
    business_rules["123"] = df.projectcode.isna()
    business_rules["301"] = ~df.opleverdatum.isna() & df.opleverstatus.isin(["0", "14"])
    business_rules["303"] = df.kabelid.isna() & (
        df.postcode.isna() | df.huisnummer.isna()
    )
    business_rules["304"] = no_errors_series  # geen column Kavel...
    business_rules["306"] = ~df.kabelid.isna() & df.opleverstatus.isin(
        ["90", "91", "96", "97", "98", "99"]
    )
    business_rules["308"] = no_errors_series  # geen HLopleverdatum...
    business_rules["309"] = no_errors_series  # geen doorvoerafhankelijk aanwezig...

    business_rules[
        "310"
    ] = no_errors_series  # (~df.KabelID.isna() & df.Areapop.isna())  # strengID != KabelID?
    business_rules["311"] = df.redenna.isna() & ~df.opleverstatus.isin(
        ["2", "10", "50"]
    )
    business_rules["501"] = ~df.postcode.str.match(r"\d{4}[a-zA-Z]{2}").fillna(False)
    business_rules["502"] = no_errors_series  # niet te checken, geen toegang tot CLR
    business_rules[
        "503"
    ] = no_errors_series  # date is already present in different format...yyyy-mm-dd??
    business_rules[
        "504"
    ] = no_errors_series  # date is already present in different format...yyyy-mm-dd??
    business_rules["506"] = ~df.opleverstatus.isin(
        [
            "0",
            "1",
            "2",
            "4",
            "5",
            "6",
            "7," "8",
            "9",
            "10",
            "11",
            "12",
            "13",
            "14",
            "15",
            "30",
            "31",
            "33",
            "34",
            "35",
            "50",
            "90",
            "91",
            "96",
            "97",
            "98",
            "99",
        ]
    )
    business_rules[
        "508"
    ] = no_errors_series  # niet te checken, geen toegang tot Areapop

    def check_numeric_and_lenght(
        series: pd.Series, min_length=1, max_length=100, fillna=True
    ):
        """
        Checks if the number of digits is within a range. Empty values will be evaluated the fillna parameter describes.
        Args:
            series: A series of values
            min_length: Minimal length
            max_length: Maximum length
            fillna: True or False.

        Returns:

        """
        return (
            (series.str.len() > max_length)
            | (series.str.len() < min_length)
            | ~(series.str.isnumeric().fillna(fillna))
        )

    business_rules["509"] = check_numeric_and_lenght(df.kastrij, max_length=2)
    business_rules["510"] = check_numeric_and_lenght(df.kast, max_length=4)
    business_rules["511"] = check_numeric_and_lenght(df.odf, max_length=5)
    business_rules["512"] = check_numeric_and_lenght(df.odfpos, max_length=2)
    business_rules["513"] = check_numeric_and_lenght(df.catv, max_length=5)
    business_rules["514"] = check_numeric_and_lenght(df.catvpos, max_length=3)

    business_rules["516"] = no_errors_series  # cannot check
    business_rules[
        "517"
    ] = no_errors_series  # date is already present in different format...yyyy-mm-dd??
    business_rules["518"] = ~df.toestemming.isin(["Ja", "Nee", np.nan, None])
    business_rules["519"] = ~df.soort_bouw.isin(
        ["Laag", "Hoog", "Duplex", "Woonboot", "Onbekend"]
    )
    business_rules["520"] = (
        df.ftu_type.isna() & df.opleverstatus.isin(["2", "10"])
    ) | (
        ~df.ftu_type.isin(
            [
                "FTU_GN01",
                "FTU_GN02",
                "FTU_PF01",
                "FTU_PF02",
                "FTU_TY01",
                "FTU_ZS_GN01",
                "FTU_TK01",
                "Onbekend",
            ]
        )
    )
    business_rules["521"] = df.toelichting_status.str.len() < 3
    business_rules["522"] = no_errors_series  # Civieldatum not present in our FC dump
    business_rules["524"] = no_errors_series  # Kavel not present in our FC dump
    business_rules[
        "527"
    ] = no_errors_series  # HL opleverdatum not present in our FC dump
    business_rules["528"] = ~df.redenna.isin(
        [
            np.nan,
            None,
            "R0",
            "R1",
            "R2",
            "R3",
            "R4",
            "R5",
            "R6",
            "R7",
            "R8",
            "R9",
            "R10",
            "R11",
            "R12",
            "R13",
            "R14",
            "R15",
            "R16",
            "R17",
            "R18",
            "R19",
            "R20",
            "R21",
            "R22",
        ]
    )
    business_rules["531"] = no_errors_series  # strengID niet aanwezig in deze FCdump
    # if df[~df.CATVpos.isin(['999'])].shape[0] > 0:
    #     business_rules['532'] = [df.sleutel[el] for el in df.ODFpos.index
    #                                 if ((int(df.CATVpos[el]) - int(df.ODFpos[el]) != 1) &
    #                                     (int(df.CATVpos[el]) != '999')) |
    #                                    (int(df.ODFpos[el]) % 2 == [])]
    business_rules[
        "533"
    ] = no_errors_series  # Doorvoerafhankelijkheid niet aanwezig in deze FCdump
    business_rules[
        "534"
    ] = no_errors_series  # geen toegang tot CLR om te kunnen checken
    business_rules["535"] = df.toelichting_status.str.contains(",").fillna(False)
    business_rules["536"] = df.kabelid.str.len() < 3
    business_rules["537"] = no_errors_series  # Blok not present in our FC dump
    business_rules[
        "701"
    ] = no_errors_series  # Kan niet gecheckt worden, hebben we vorige waarde voor nodig...
    business_rules["702"] = ~df.odf.isna() & df.opleverstatus.isin(
        ["90", "91", "96", "97", "98", "99"]
    )
    business_rules[
        "707"
    ] = no_errors_series  # Kan niet gecheckt worden, hebben we vorige waarde voor nodig...
    business_rules["708"] = (
        df.opleverstatus.isin(["90"]) & ~df.redenna.isin(["R15", "R16", "R17"])
    ) | (df.opleverstatus.isin(["91"]) & ~df.redenna.isin(["R12", "R13", "R14", "R21"]))
    # business_rules['709'] = ((df.ODF + df.ODFpos).duplicated(keep='last'))  # klopt dit?
    business_rules["710"] = (
        ~df.kabelid.isna()
        & ~df.adres.isna()
        & (df.kabelid + df.adres).duplicated(keep=False)
    )
    # business_rules['711'] = (~df.CATV.isin(['999']) | ~df.CATVpos.isin(['999']))  # wanneer PoP 999?
    business_rules["713"] = no_errors_series  # type bouw zit niet in onze FC dump
    # if df[df.ODF.isin(['999']) & df.ODFpos.isin(['999']) & df.CATVpos.isin(['999']) & df.CATVpos.isin(['999'])].shape[0] > 0:
    #     business_rules['714'] = df[~df.ODF.isin(['999']) | ~df.ODFpos.isin(['999']) | ~df.CATVpos.isin(['999']) |
    #                                 ~df.CATVpos.isin(['999'])].sleutel.to_list()
    business_rules["716"] = no_errors_series  # niet te checken, geen toegang tot SIMA
    business_rules["717"] = no_errors_series  # type bouw zit niet in onze FC dump
    business_rules[
        "719"
    ] = no_errors_series  # kan alleen gecheckt worden met geschiedenis
    business_rules[
        "721"
    ] = no_errors_series  # niet te checken, geen Doorvoerafhankelijkheid in FC dump
    business_rules["723"] = (
        (df.redenna.isin(["R15", "R16", "R17"]) & ~df.opleverstatus.isin(["90"]))
        | (
            df.redenna.isin(["R12", "R12", "R14", "R21"])
            & ~df.opleverstatus.isin(["91"])
        )
        | (df.opleverstatus.isin(["90"]) & df.redenna.isin(["R2", "R11"]))
    )
    business_rules["724"] = ~df.opleverdatum.isna() & df.redenna.isin(
        ["R0", "R19", "R22"]
    )
    business_rules[
        "725"
    ] = no_errors_series  # geen zicht op vraagbundelingsproject of niet
    business_rules[
        "726"
    ] = no_errors_series  # niet te checken, geen HLopleverdatum aanwezig
    business_rules["727"] = df.opleverstatus.isin(["50"])
    business_rules["728"] = no_errors_series  # voorkennis nodig over poptype

    business_rules[
        "729"
    ] = no_errors_series  # kan niet checken, vorige staat FC voor nodig
    business_rules[
        "90x"
    ] = no_errors_series  # kan niet checken, extra info over bestand nodig!

    errors_FC_BC = defaultdict(dict)

    for err_no, mask in business_rules.items():
        g_df = df[mask].groupby(by="project")["sleutel"].apply(list)
        for p, sleutels in g_df.items():
            errors_FC_BC[p][err_no] = sleutels

    n_err = {}
    for plaats, err_sleutels in errors_FC_BC.items():
        total_sleutels = set()
        for err, sleutels in err_sleutels.items():
            total_sleutels.update(sleutels)
        n_err[plaats] = len(set(total_sleutels))

    return n_err, errors_FC_BC


# TODO: Documentation by Casper van Houten
def linear_regression(data):
    fit_range = data.day_count.to_list()
    slope, intersect = np.polyfit(fit_range, data, 1)
    return slope[0], intersect[0]


def get_secret(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = client.secret_version_path(project_id, secret_id, version_id)
    response = client.access_secret_version(name)
    payload = response.payload.data.decode("UTF-8")
    return payload


def get_database_engine():
    """
    Construct an SQLAlchemy Engine based on the config file.

    Returns:
        An SQLAlchemy Engine instance
    """

    if "db_ip" in config.database:
        SACN = "mysql+mysqlconnector://{}:{}@{}:3306/{}?charset=utf8&ssl_ca={}&ssl_cert={}&ssl_key={}".format(
            config.database["db_user"],
            get_secret(config.database["project_id"], config.database["secret_name"]),
            config.database["db_ip"],
            config.database["db_name"],
            config.database["server_ca"],
            config.database["client_ca"],
            config.database["client_key"],
        )
    else:
        SACN = (
            "mysql+pymysql://{}:{}@/{}?unix_socket=/cloudsql/{}:europe-west1:{}".format(
                config.database["db_user"],
                get_secret(
                    config.database["project_id"], config.database["secret_name"]
                ),
                config.database["db_name"],
                config.database["project_id"],
                config.database["instance_id"],
            )
        )

    return create_engine(SACN, pool_recycle=3600)


def get_timestamp_of_period(freq: str, period="next"):
    """
    This functions returns the corresponding timestamp of past, current or next week or month based a frequency

    Args:
        freq (str): frequency used to determine the time delta used to look forward or backwards.
                    With 'W-MON' the delta is a week, with 'MS' the delta is a month and with 'D' the delta is a day
        period (str): period that will be returned; Last period, current period or next period.

    Raises:
        NotImplementedError: there is no method implemented for this type of frequency.

    Returns:
        Index of chosen period (pd.Timestamp)
    """
    period_options = {}
    now = pd.Timestamp.now()

    if freq == "D":
        period_options["last"] = pd.to_datetime(now.date() + relativedelta(days=-1))
        period_options["current"] = pd.to_datetime(now.date())
        period_options["next"] = pd.to_datetime(now.date() + relativedelta(days=1))
    elif freq == "W-MON":
        period_options["last"] = pd.to_datetime(
            now.date() + relativedelta(days=-7 - now.weekday())
        )
        period_options["current"] = pd.to_datetime(
            now.date() - relativedelta(days=now.weekday())
        )
        period_options["next"] = pd.to_datetime(
            now.date() + relativedelta(days=7 - now.weekday())
        )
    elif freq == "MS":
        period_options["last"] = pd.Timestamp(now.year, now.month, 1) + relativedelta(
            months=-1
        )
        period_options["current"] = pd.Timestamp(now.year, now.month, 1)
        period_options["next"] = pd.Timestamp(now.year, now.month, 1) + relativedelta(
            months=1
        )
    else:
        raise NotImplementedError(
            "There is no output period implemented for this frequency {}".format(freq)
        )

    period_timestamp = period_options.get(period)
    if period_timestamp:
        return period_timestamp
    else:
        raise NotImplementedError(
            f'The selected period "{period}" '
            'is not valid. Choose "last", "current" or "next"'
        )


def cluster_reden_na(label, clusters):
    """
    Retrieves the relevant cluster of a label, given a set of clusters.
    Args:
        label: Current, unclustered label of the data
        clusters: A dictionary of clusters, keys being the name of the cluster, values being the labels in the cluster.

    Returns: The cluster of the data.

    """
    for k, v in clusters.items():
        if label in v:
            return k
    # raise ValueError(f'No label found for {label}')
