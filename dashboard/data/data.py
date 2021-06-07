from datetime import datetime

import pandas as pd

import config
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
        "Realisatie": ["RealisationHPcivielIndicator", "RealisationHPendIndicator"],
        "Planning": [
            "PlanningHPcivielIndicator",
            "PlanningHPendIndicator",
        ],
        "Voorspelling": ["linenotavailable", "PrognoseHPendIndicator"],
        "Werkvoorraad": ["linenotavailable", "WerkvoorraadHPendIndicator"],
        "HC / HPend": [
            "linenotavailable",
            "RealisationHCIndicator",
            "RealisationHPendIndicator",
        ],
        "Ratio <12 weken": [
            "linenotavailable",
            "RealisationHCOnTimeIndicator",
            "RealisationHCIndicator",
        ],
        "Leverbetrouwbaarheid": [
            "linenotavailable",
            "leverbetrouwbaarheid",
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
            if values[2] != "n.v.t.":
                values[1] = str(round(int(values[1]) / int(values[2]), 2))
                if (title == "HC / HPend") and (client == "tmobile"):
                    values[1] = "n.v.t."

        parameters_global_info_list.append(
            dict(
                id_="",
                title=title,
                text1="HPciviel: ",
                text2="HPend: " if title != "Ratio <12 weken" else "HC: ",
                value1=values[0],
                value2=values[1],
            )
        )

    return parameters_global_info_list


def fetch_data_for_month_overview(year, client):
    lines = [
        ["target", "InternalTargetHPendLine"],
        ["realisatie", "RealisationHPendIndicator"],
        ["planning", "PlanningHPendIndicator"],
        ["prognose", "PrognoseHPendIndicator"],
    ]
    df = pd.DataFrame(index=pd.date_range(start=year, freq="MS", periods=12))
    for line in lines:
        df[line[0]] = collection.get_month_series_from_document(
            collection="Indicators",
            year=year,
            line=line[1],
            client=client,
            project="client_aggregate",
        )

    return df


def fetch_data_for_week_overview(year, client):
    lines = [
        ["target", "InternalTargetHPendLine"],
        ["realisatie", "RealisationHPendIndicator"],
        ["planning", "PlanningHPendIndicator"],
        ["prognose", "PrognoseHPendIndicator"],
    ]
    df = pd.DataFrame(index=pd.date_range(start=year, freq="W-MON", periods=52))
    for line in lines:
        df[line[0]] = collection.get_week_series_from_document(
            collection="Indicators",
            year=year,
            line=line[1],
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
        if period == "overview":  # this happens when the reset button is used.
            period = "year"

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

    redenna_by_period = collection.get_redenna_overview_from_document(
        collection="Indicators",
        date=date,
        period=period,
        client=client,
        project="client_aggregate",
    )

    # Sorted the cluster redenna dict here, so that the pie chart pieces have the proper color:
    data = dict(sorted(redenna_by_period.get(date, dict()).items()))

    return data, title


def fetch_data_for_indicator_boxes(project, client):
    this_week = datetime.now().isocalendar()[1]
    indicator_types = {
        f"Realisatie HPend w {str(this_week - 1)}": [
            "RealisationHPendIndicator",
            "InternalTargetHPendLine",
        ],
        f"Realisatie HPend w {str(this_week)}": [
            "RealisationHPendIndicator",
            "InternalTargetHPendLine",
        ],
        f"Realisatie HPciviel w {str(this_week - 1)}": [
            "RealisationHPcivielIndicator",
            "InternalTargetHPcivielLine",
        ],
        f"Realisatie HPciviel w {str(this_week)}": [
            "RealisationHPcivielIndicator",
            "InternalTargetHPcivielLine",
        ],
        "HC / HPend": [
            "RealisationHCIndicatorIntegrated",
            "RealisationHPendIndicatorIntegrated",
        ],
        "Leverbetrouwbaarheid": [
            "leverbetrouwbaarheid",
            "n.v.t.",
        ],
    }

    info_list = []
    for title in indicator_types:
        values = []
        gauge_type = "bullet"
        sub_title = "Target: "
        for line in indicator_types[title]:
            if (title[-2:] == str(this_week)) | (title == "Leverbetrouwbaarheid"):
                which_week = "current_week"
            else:
                which_week = "last_week"
            if title == "HC / HPend":
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
        if title == "HC / HPend":
            if values[1] != 0:
                values[0] = round(values[0] / values[1], 2)
            values[1] = 0.9
            gauge_type = "speedo"

        if title == "Leverbetrouwbaarheid":
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


def fetch_data_for_indicator_boxes_tmobile(project, client):
    indicator_types = {
        "HC open op tijd": [
            "< 8 weken",
            "on_time-hc_aanleg",
            "HCopenOnTime",
        ],
        "HC open laat": [
            "> 8 weken < 12 weken",
            "late-hc_aanleg",
            "HCopenLate",
        ],
        "HC open te laat": [
            "> 12 weken",
            "too_late-hc_aanleg",
            "HCopenTooLate",
        ],
        "Ratio op tijd aangesloten": [
            " ",
            "ratio-12-weeks",
            "RealisationHCIntegratedTmobileOnTimeIndicator",
            "RealisationHCIndicatorIntegrated",
        ],
        "Leverbetrouwbaarheid": [
            " ",
            "leverbetrouwbaarheid",
            "leverbetrouwbaarheid",
        ],
        "Patch only open op tijd": [
            "< 8 weken",
            "on_time-patch_only",
            "PatchOnlyOnTime",
        ],
        "Patch only open laat": [
            "> 8 weken < 12 weken",
            "late-patch_only",
            "PatchOnlyLate",
        ],
        "Patch only open te laat": [
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
        if title == "Ratio op tijd aangesloten":
            value2 = collection.get_year_value_from_document(
                collection="Indicators",
                year=year,
                line=indicator_types[title][3],
                client=client,
                project=project,
            )
            if (value2 != 0) & (value != "n.v.t."):
                value = round(value / value2 * 100) / 100

        info_list.append(
            dict(
                value=value if not isinstance(value, str) else 0,
                previous_value=None,
                title=title,
                sub_title=subtitle,
                font_color="black",
                id=f"indicator-{indicator_types[title][1]}-{client}",
                gauge_type="standard",
            )
        )
    return info_list[0:5], info_list[5:]


def fetch_data_for_redenna_modal(project, client, indicator_type, wait_category):
    pie_dict = collection.get_redenna_modal_from_document(
        collection="Indicators",
        graph_name=f"RedenNA_{wait_category}_{indicator_type}",
        client=client,
        project=project,
    )["clusters"]
    return dict(sorted(pie_dict.items()))


def fetch_df_aggregate(project, client, indicator_type, wait_category):
    doc = collection.get_redenna_modal_from_document(
        collection="Indicators",
        graph_name=f"RedenNA_{wait_category}_{indicator_type}",
        client=client,
        project=project,
    )
    return pd.DataFrame(doc["df_aggregate"])


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


def fetch_data_for_status_redenna_piechart(project_name, client, click_filter=None):

    counts = pd.DataFrame(
        collection.get_document(
            collection="Indicators",
            graph_name="ActualStatusBarChartIndicator",
            project=project_name,
            client=client,
        )
    )

    if not counts.empty:
        clusters = config.client_config[client]["clusters_reden_na"]
        cluster_types = pd.CategoricalDtype(
            categories=list(clusters.keys()), ordered=True
        )
        counts["cluster_redenna"] = counts["cluster_redenna"].astype(cluster_types)

        mask = pd.Series([True]).repeat(len(counts.index)).values
        if click_filter:
            for col, value in click_filter.items():
                mask = mask & (counts[col] == value)
            cols_filter = list(click_filter.keys())
        else:
            cols_filter = []

        cols = list(dict.fromkeys(cols_filter + ["cluster_redenna"])) + ["laagbouw"]
        result = counts[mask][cols + ["count"]].groupby(cols).sum().reset_index()

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

    else:
        total = pd.DataFrame()
        laagbouw = pd.DataFrame()
        hoogbouw = pd.DataFrame()

    return total, laagbouw, hoogbouw


def fetch_data_for_status_barchart(project_name, client, click_filter=None):

    counts = pd.DataFrame(
        collection.get_document(
            collection="Indicators",
            graph_name="ActualStatusBarChartIndicator",
            project=project_name,
            client=client,
        )
    )

    categories = [
        "schouw_status",
        "bis_status",
        "lasAP_status",
        "lasDP_status",
        "HAS_status",
    ]

    phase_category = pd.CategoricalDtype(categories=categories)

    status_category = pd.CategoricalDtype(
        categories=[
            "niet_opgeleverd",
            "ingeplanned",
            "opgeleverd",
            "opgeleverd_zonder_hc",
        ]
    )

    if not counts.empty:
        mask = pd.Series([True]).repeat(len(counts.index)).values
        if click_filter:
            for col, value in click_filter.items():
                mask = mask & (counts[col] == value)
            cols_filter = list(click_filter.keys())
        else:
            cols_filter = []

        laagbouw_matrix = []
        hoogbouw_matrix = []

        for category in categories:
            cols = list(dict.fromkeys(cols_filter + [category])) + ["laagbouw"]
            result = counts[mask][cols + ["count"]].groupby(cols).sum().reset_index()

            lb = result[result.laagbouw][[category, "count"]]
            for i, row in lb.iterrows():
                laagbouw_matrix.append([category] + row.values.tolist())

            hb = result[~result.laagbouw][[category, "count"]]
            for i, row in hb.iterrows():
                hoogbouw_matrix.append([category] + row.values.tolist())

        lb_df = pd.DataFrame(laagbouw_matrix, columns=["phase", "status", "count"])
        lb_df["status"] = lb_df.status.astype(status_category)
        lb_df["phase"] = lb_df.phase.astype(phase_category)
        lb_df = lb_df.groupby(by=["phase", "status"]).sum().reset_index()
        if lb_df.empty:
            lb_df["count"] = 0
        else:
            lb_df["count"] = lb_df["count"].fillna(0)
        data_laagbouw = lb_df

        hb_df = pd.DataFrame(hoogbouw_matrix, columns=["phase", "status", "count"])
        hb_df["status"] = hb_df.status.astype(status_category)
        hb_df["phase"] = hb_df.phase.astype(phase_category)
        hb_df = hb_df.groupby(by=["phase", "status"]).sum().reset_index()
        if hb_df.empty:
            hb_df["count"] = 0
        else:
            hb_df["count"] = hb_df["count"].fillna(0)
        data_hoogbouw = hb_df

    else:
        data_laagbouw = pd.DataFrame()
        data_hoogbouw = pd.DataFrame()

    return data_laagbouw, data_hoogbouw


def fetch_data_productionstatus(project, client, freq, phase_name):
    indicator_values = {}
    timeseries = {}
    name_indicator = {
        "target": "Target",
        "poc_verwacht": "Verwacht verloop",
        "poc_ideal": "Ideaal verloop",
        "work_stock": "Werkvoorraad (totale productie)",
        "work_stock_amount": "Hoeveelheid Werkvoorraad",
    }
    line_graph_bool = False
    for key in [
        "target",
        "work_stock",
        "poc_verwacht",
        "poc_ideal",
        "work_stock_amount",
    ]:
        indicator_dict = collection.get_document(
            collection="Lines",
            line=key + "_indicator",
            project=project,
            client=client,
            phase=phase_name,
        )
        if indicator_dict:
            line_graph_bool = True
            indicator_values[name_indicator[key]] = int(indicator_dict["next_" + freq])
            timeseries[name_indicator[key]] = pd.Series(
                indicator_dict["series_" + freq]
            )
        else:
            indicator_values[name_indicator[key]] = 0
            timeseries[name_indicator[key]] = pd.Series()

    if indicator_values["Hoeveelheid Werkvoorraad"] < 0:
        indicator_values["Hoeveelheid Werkvoorraad"] = 0
    if phase_name == "geulen":
        indicator_values["Hoeveelheid Werkvoorraad"] = None

    del timeseries["Hoeveelheid Werkvoorraad"]

    return indicator_values, timeseries, line_graph_bool
