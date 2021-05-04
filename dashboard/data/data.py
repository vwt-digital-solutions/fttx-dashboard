from collections import namedtuple
from datetime import datetime

import pandas as pd

import config
from app import toggles
from data import collection


def fetch_data_for_performance_graph(client):
    return collection.get_data_performance_graph(
        collection="Indicators", client=client, graph_name="data_for_performance_graph"
    )


def fetch_data_for_progress_HPend_chart(client, project):
    data = {}
    data["prognose"] = collection.get_week_series_from_document(
        collection="Indicators",
        client=client,
        project=project,
        line="PrognoseHPendIndicatorIntegrated",
    )
    data["target"] = collection.get_week_series_from_document(
        collection="Indicators",
        client=client,
        project=project,
        line="InternalTargetHPendLineIntegrated",
    )
    data["realisatie"] = collection.get_week_series_from_document(
        collection="Indicators",
        client=client,
        project=project,
        line="RealisationHPendIndicatorIntegrated",
    )
    return data


def fetch_data_for_overview_boxes(client, year):
    lines_for_in_boxes = {
        "Internal Target": [
            "InternalTargetHPcivielLine",
            "InternalTargetHPendLine",
        ],
        "Client Target": ["ClientTarget", "ClientTarget"],
        "Realisatie": [
            "RealisationHPcivielIndicator",
            "RealisationHPendIndicator"
            if client != "tmobile"
            else "RealisationHPendTmobileIndicator",
        ],
        "Planning": [
            "PlanningHPcivielIndicator",
            "PlanningHPendIndicator",
        ],
        "Voorspelling": ["linenotavailable", "PrognoseHPendIndicator"],
        "Werkvoorraad": ["linenotavailable", "WerkvoorraadHPendIndicator"],
        "Ratio HC / HPend": [
            "linenotavailable",
            "RealisationHCIndicator",
            "RealisationHPendIndicator",
        ],
        "Ratio <12 weken": [
            "linenotavailable",
            "RealisationHPendOnTimeIndicator",
            "RealisationHPendTmobileIndicator",
        ],
    }

    parameters_global_info_list = []
    for title in lines_for_in_boxes:
        values = []
        for indicator in lines_for_in_boxes[title]:
            values.append(
                str(
                    collection.get_year_value_from_document(
                        collection="Indicators",
                        year=year,
                        line=indicator,
                        client=client,
                        project="client_aggregate",
                    )
                )
            )

        # exception for calculation of ratio's
        if (len(values) == 3) & (values[1] != "n.v.t."):
            values[1] = str(round(int(values[1]) / int(values[2]), 2))

        parameters_global_info_list.append(
            dict(
                id_="",
                title=title,
                text1="HPciviel: ",
                text2="HPend: ",
                value1=values[0],
                value2=values[1],
            )
        )

    return parameters_global_info_list


def fetch_data_for_month_overview(year, client):
    lines = [
        "InternalTargetHPendLine",
        "RealisationHPendIndicator"
        if client != "tmobile"
        else "RealisationHPendTmobileIndicator",
        "PlanningHPendIndicator",
        "PrognoseHPendIndicator",
    ]
    df = pd.DataFrame(index=pd.date_range(start=year, freq="MS", periods=12))
    for line in lines:
        df[line] = collection.get_month_series_from_document(
            collection="Indicators",
            year=year,
            line=line,
            client=client,
            project="client_aggregate",
        )

    return df


def fetch_data_for_week_overview(year, client):
    lines = [
        "InternalTargetHPendLine",
        "RealisationHPendIndicator"
        if client != "tmobile"
        else "RealisationHPendTmobileIndicator",
        "PlanningHPendIndicator",
        "PrognoseHPendIndicator",
    ]
    df = pd.DataFrame(index=pd.date_range(start=year, freq="W-MON", periods=52))
    for line in lines:
        df[line] = collection.get_week_series_from_document(
            collection="Indicators",
            year=year,
            line=line,
            client=client,
            project="client_aggregate",
        )

    return df


def fetch_data_for_project_info_table(client):
    return collection.get_document(
        collection="ProjectInfo", graph_name="project_dates", client=client
    )


def fetch_data_for_redenna_overview(ctx, year, client):
    def get_date_and_period_and_title(ctx, year):
        """
        This function returns the settings to plot a pie chart based on annual, monthly or weekly views.

        :param ctx: A dash callback, triggered by clicking in Jaaroverzicht or Maandoverzicht graphs
        :param year: The current year, as set by the year selector dropdown
        :return: date, period, title
        """
        dutch_month_list = [
            "jan",
            "feb",
            "maa",
            "apr",
            "mei",
            "jun",
            "jul",
            "aug",
            "sep",
            "okt",
            "nov",
            "dec",
        ]
        period, _, _ = ctx.triggered[0]["prop_id"].partition("-")

        if period == "year":
            date = f"{year}-01-01"
            title = f"Reden na bij has ingepland voor het jaar {year}"
        else:
            date = ctx.triggered[0]["value"]["points"][0]["customdata"]
            if period == "week":
                title = f"Reden na bij has ingepland voor de week {date}"
            if period == "month":
                extract_month_in_dutch = dutch_month_list[int(date.split("-")[1]) - 1]
                title = f"Reden na bij has ingepland voor de maand {extract_month_in_dutch} {year}"
        return date, period, title

    date, period, title = get_date_and_period_and_title(ctx, year)

    if toggles.transform_frontend_newindicator:
        redenna_by_period = collection.get_redenna_overview_from_document(
            collection="Indicators",
            date=date,
            period=period,
            client=client,
            project="client_aggregate",
        )
    else:
        redenna_by_period = collection.get_document(
            collection="Data", client=client, graph_name=f"redenna_by_{period}"
        )
        if period == "year":
            date = date[0:4] + "-12-31"

    # Sorted the cluster redenna dict here, so that the pie chart pieces have the proper color:
    print(date)
    data = dict(sorted(redenna_by_period.get(date, dict()).items()))

    return data, title


def fetch_data_for_indicator_boxes(project, client):
    this_week = datetime.now().isocalendar()[1]
    indicator_types = {
        f"Realisatie HPend week {str(this_week - 1)}": [
            "RealisationHPendIndicator",
            "InternalTargetHPendLine",
        ],
        f"Realisatie HPend week {str(this_week)}": [
            "RealisationHPendIndicator",
            "InternalTargetHPendLine",
        ],
        f"Realisatie HPciviel week {str(this_week - 1)}": [
            "RealisationHPcivielIndicator",
            "InternalTargetHPcivielLine",
        ],
        f"Realisatie HPciviel week {str(this_week)}": [
            "RealisationHPcivielIndicator",
            "InternalTargetHPcivielLine",
        ],
        f"Ratio HC / HPend week {str(this_week)}": [
            "RealisationHCIndicatorIntegrated",
            "RealisationHPendIndicatorIntegrated",
        ],
    }

    info_list = []
    for title in indicator_types:
        values = []
        gauge_type = "bullet"
        sub_title = "Target: "
        for line in indicator_types[title]:
            if title[-2:] == str(this_week):
                which_week = "current_week"
            else:
                which_week = "last_week"
            if title == f"Ratio HC / HPend week {str(this_week)}":
                which_week = "max_value_on_weekly_basis"
            values.append(
                collection.get_week_value_from_document(
                    collection="Indicators",
                    which_week=which_week,
                    line=line,
                    client=client,
                    project=project,
                )
            )

        # exception for calculation of ratio's
        if title == f"Ratio HC / HPend week {str(this_week)}":
            if values[1] != 0:
                values[0] = round(values[0] / values[1], 2)
            values[1] = 0.9
            gauge_type = "speedo"

        info_list.append(
            dict(
                value=values[0],
                value2=values[1],
                previous_value=None,
                title=title,
                sub_title=sub_title,
                font_color="black",
                gauge_type=gauge_type,
            )
        )
    return info_list


# def fetch_data_for_redenna_project(project):
#     redenna_project = collection.get_redenna_overview_from_document(
#         collection="Indicators",
#         date=date,
#         period=period,
#         client=client,
#         project=project,
#     )
#     return redenna_project


def fetch_data_for_indicator_boxes_tmobile(project, client):
    indicator_types = {
        "Openstaand HC aanleg op tijd": [
            "< 8 weken",
            "on_time-hc_aanleg",
            "HCopenOnTime",
        ],
        "Openstaand HC aanleg beperkte tijd": [
            "> 8 weken < 12 weken",
            "late-hc_aanleg",
            "HCopenLate",
        ],
        "Openstaand HC aanleg te laat": [
            "> 12 weken",
            "too_late-hc_aanleg",
            "HCopenTooLate",
        ],
        "Ratio op tijd gesloten orders": [
            " ",
            "ratio-12-weeks",
            "RealisationHPendOnTimeIndicatorIntegrated",
            "RealisationHPendIntegratedTmobileIndicator",
        ],
        "Openstaand patch only op tijd": [
            "< 8 weken",
            "on_time-patch_only",
            "PatchOnlyOnTime",
        ],
        "Openstaand patch only beperkte tijd": [
            "> 8 weken < 12 weken",
            "late-patch_only",
            "PatchOnlyLate",
        ],
        "Openstaand patch only te laat": [
            "> 12 weken",
            "too_late-patch_only",
            "PatchOnlyTooLate",
        ],
        "Werkvoorraad HAS": [
            " ",
            "werkvoorraad-has",
            "WerkvoorraadHPendIndicator",
        ],
    }

    info_list = []
    year = str(datetime.now().year)
    for title in indicator_types:
        subtitle = indicator_types[title][0]
        line = indicator_types[title][2]
        value = collection.get_year_value_from_document(
            collection="Indicators",
            year=year,
            line=line,
            client=client,
            project=project,
        )

        # exception for calculation of ratio's
        if title == "Ratio op tijd gesloten orders":
            value2 = collection.get_year_value_from_document(
                collection="Indicators",
                year=year,
                line=indicator_types[title][3],
                client=client,
                project=project,
            )
            if (value2 != 0) & (value2 != "n.v.t."):
                value = round(value / value2 * 100) / 100

        info_list.append(
            dict(
                value=value if not isinstance(value, str) else 0,
                previous_value=None,
                title=title,
                sub_title=subtitle,
                font_color="black",
                id=f"indicator-{indicator_types[title][1]}-{client}",
            )
        )
    return info_list[0:4], info_list[4:]


def fetch_data_for_redenna_modal(project, client, indicator_type, wait_category):
    pie_dict = collection.get_redenna_modal_from_document(
        collection="Indicators",
        graph_name=f"RedenNA_{wait_category}_{indicator_type}",
        client=client,
        project=project,
    )["clusters"]
    return dict(sorted(pie_dict.items()))


def fetch_sleutels_figure(project, client, indicator_type, wait_category):
    sleutels = collection.get_redenna_modal_from_document(
        collection="Indicators",
        graph_name=f"RedenNA_{wait_category}_{indicator_type}",
        client=client,
        project=project,
    )["sleutels"]
    return sleutels


def fetch_data_for_overview_graphs(year: str, freq: str, period: str, client: str):
    opgeleverd_data_dict = collection.get_document(
        collection="Data",
        graph_name="realisatie_hpend",
        client=client,
        year=year,
        frequency=freq,
    )

    planning_data_dict = collection.get_document(
        collection="Data",
        graph_name="planning",
        client=client,
        year=year,
        frequency=freq,
    )
    planning_data_dict = {key: int(value) for key, value in planning_data_dict.items()}

    target_data_dict = collection.get_document(
        collection="Data", graph_name="target", client=client, year=year, frequency=freq
    )
    target_data_dict = {key: int(value) for key, value in target_data_dict.items()}

    voorspelling_data_dict = collection.get_document(
        collection="Data",
        graph_name="voorspelling",
        client=client,
        year=year,
        frequency=freq,
    )
    voorspelling_data_dict = {
        key: int(value) for key, value in voorspelling_data_dict.items()
    }

    # The following commented lines are from the old function "has_planning_by", I don't think we need them anymore:
    #
    # for tmobile the toestemming_datum is used as outlook
    # if client == 'tmobile':
    #     target_data_dict = collection.get_document(collection="Data", graph_name="toestemming",
    #                                                client=client, year=year, frequency=freq)
    # if not target_data_dict:
    #     target_data_dict['count_outlookdatum'] = opgeleverd_data_dict['opleverdatum'].copy()
    #     for el in target_data_dict['count_outlookdatum']:
    #         target_data_dict['count_outlookdatum'][el] = 0
    #     if period == 'month':
    #         target_data_dict['count_outlookdatum']['2020-11-02'] = 0
    #         target_data_dict['count_outlookdatum']['2020-12-01'] = 0
    # # temporary solution until we also have voorspelling data for T-Mobile
    # if not voorspelling_data_dict:
    #     voorspelling_data_dict['count_voorspellingdatum'] = opgeleverd_data_dict['opleverdatum'].copy()
    #     for el in voorspelling_data_dict['count_voorspellingdatum'].keys():
    #         voorspelling_data_dict['count_voorspellingdatum'][el] = 0
    #
    # # temporary solution until we also have planning data for DFN
    # if client == 'dfn':
    #     planning_data_dict['count_hasdatum'] = planning_data_dict['count_hasdatum'].copy()
    #     for el in planning_data_dict['count_hasdatum'].keys():
    #         planning_data_dict['count_hasdatum'][el] = 0

    df = (
        pd.DataFrame(
            {
                "count_hasdatum": planning_data_dict,
                "count_opleverdatum": opgeleverd_data_dict,
                "count_outlookdatum": target_data_dict,
                "count_voorspellingdatum": voorspelling_data_dict,
            }
        )
        .reset_index()
        .rename(columns={"index": "date"})
    )
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df["period"] = period
    start_date = pd.to_datetime(year + "-01-01", format="%Y-%m-%d")
    end_date = pd.to_datetime(year + "-12-31", format="%Y-%m-%d")
    mask = (df["date"] >= start_date) & (df["date"] <= end_date)
    return df[mask]


def redenna_by_completed_status(
    project_name,
    client,
    click_filter=None,
):
    RedenNADataFrames = namedtuple(
        "RedenNADataFrames", ["total", "laagbouw", "hoogbouw"]
    )  # Used to return a named tuple

    if not click_filter:
        click_filter = {}

    if project_name:
        if toggles.transform_frontend_newindicator:
            counts = pd.DataFrame(
                collection.get_document(
                    collection="Indicators",
                    graph_name="ActualStatusBarChartIndicator",
                    project=project_name,
                    client=client,
                )
            )
        else:
            counts = pd.DataFrame(
                collection.get_document(
                    collection="Data",
                    graph_name="completed_status_counts",
                    project=project_name,
                    client=client,
                )
            )
        if counts.empty:
            return None
        clusters = config.client_config[client]["clusters_reden_na"]
        cluster_types = pd.CategoricalDtype(
            categories=list(clusters.keys()), ordered=True
        )
        counts["cluster_redenna"] = counts["cluster_redenna"].astype(cluster_types)

        mask = pd.Series([True]).repeat(len(counts.index)).values
        if click_filter:
            for col, value in click_filter.items():
                mask = mask & (counts[col] == value)

        cols = list(dict.fromkeys(list(click_filter.keys()) + ["cluster_redenna"]))
        cols.append("laagbouw")
        cols_to_see = cols + ["count"]
        result = counts[mask][cols_to_see].groupby(cols).sum().reset_index()
        total = (
            result.groupby("cluster_redenna")
            .sum()
            .reset_index()[["cluster_redenna", "count"]]
        )
        laagbouw = (
            result[result.laagbouw]
            .groupby("cluster_redenna")
            .sum()
            .reset_index()[["cluster_redenna", "count"]]
        )
        hoogbouw = (
            result[~result.laagbouw]
            .groupby("cluster_redenna")
            .sum()
            .reset_index()[["cluster_redenna", "count"]]
        )

        return RedenNADataFrames(total, laagbouw, hoogbouw)

    return None, None


def completed_status_counts(project_name, client, click_filter=None):  # noqa: C901
    StatusCountDataFrames = namedtuple(
        "StatusCountDataFrames", ["laagbouw", "hoogbouw"]
    )  # Used to return a named tuple

    if not click_filter:
        click_filter = {}

    categories = [
        "schouw_status",
        "bis_status",
        "lasAP_status",
        "lasDP_status",
        "HAS_status",
    ]

    if project_name:
        if toggles.transform_frontend_newindicator:
            counts = pd.DataFrame(
                collection.get_document(
                    collection="Indicators",
                    graph_name="ActualStatusBarChartIndicator",
                    project=project_name,
                    client=client,
                )
            )
        else:
            counts = pd.DataFrame(
                collection.get_document(
                    collection="Data",
                    graph_name="completed_status_counts",
                    project=project_name,
                    client=client,
                )
            )
        if counts.empty:
            return None
        clusters = config.client_config[client]["clusters_reden_na"]
        cluster_types = pd.CategoricalDtype(
            categories=list(clusters.keys()), ordered=True
        )
        counts["cluster_redenna"] = counts["cluster_redenna"].astype(cluster_types)

        mask = pd.Series([True]).repeat(len(counts.index)).values
        if click_filter:
            for col, value in click_filter.items():
                mask = mask & (counts[col] == value)

        laagbouw_matrix = []
        hoogbouw_matrix = []

        for category in categories:
            cols = list(dict.fromkeys(list(click_filter.keys()) + [category]))
            cols.append("laagbouw")
            cols_to_see = cols + ["count"]
            result = counts[mask][cols_to_see].groupby(cols).sum().reset_index()
            lb = result[result.laagbouw][[category, "count"]]
            hb = result[~result.laagbouw][[category, "count"]]
            for i, row in lb.iterrows():
                laagbouw_matrix.append([category] + row.values.tolist())
            for i, row in hb.iterrows():
                hoogbouw_matrix.append([category] + row.values.tolist())

        lb_df = pd.DataFrame(laagbouw_matrix, columns=["phase", "status", "count"])
        hb_df = pd.DataFrame(hoogbouw_matrix, columns=["phase", "status", "count"])

        status_category = pd.CategoricalDtype(
            categories=[
                "niet_opgeleverd",
                "ingeplanned",
                "opgeleverd",
                "opgeleverd_zonder_hc",
            ]
        )
        phase_category = pd.CategoricalDtype(categories=categories)
        lb_df["status"] = lb_df.status.astype(status_category)
        lb_df["phase"] = lb_df.phase.astype(phase_category)
        lb_df = lb_df.groupby(by=["phase", "status"]).sum().reset_index()
        if lb_df.empty:
            lb_df["count"] = 0
        else:
            lb_df["count"] = lb_df["count"].fillna(0)
        hb_df["status"] = hb_df.status.astype(status_category)
        hb_df["phase"] = hb_df.phase.astype(phase_category)
        hb_df = hb_df.groupby(by=["phase", "status"]).sum().reset_index()
        if hb_df.empty:
            hb_df["count"] = 0
        else:
            hb_df["count"] = hb_df["count"].fillna(0)

        return StatusCountDataFrames(lb_df, hb_df)

    return StatusCountDataFrames(
        pd.DataFrame(columns=["phase", "status", "count"]),
        pd.DataFrame(columns=["phase", "status", "count"]),
    )
