from functools import reduce
import pandas as pd


def add_weeknumber(input_column):
    input_column = input_column.dt.strftime('%V')
    return input_column


def calculate_voorraadvormend(df):
    voorraad_df = df[(~df.hasdatum.isna()) & df.plan_status.str.match("-1")]
    voorraad_project_counts = voorraad_df[["project", "sleutel"]].groupby(by=["project"]).count()

    projects = df.project.unique()
    voorraad_project_counts = (
        voorraad_project_counts
        .reindex(projects, fill_value=0)
        .reset_index()
        .rename(columns={"sleutel": "voorraad_count"})
    )

    totals = voorraad_project_counts.sum()
    totals.project = "all"
    totals.name = 'totals'

    voorraad_project_counts = voorraad_project_counts.append(totals).set_index("project").to_dict()[
        'voorraad_count'].copy()
    return voorraad_project_counts


def counts_by_time_period(df: pd.DataFrame, freq: str = 'W-MON') -> dict:
    """
    Set the freq using: https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
    We commonly use:
        'MS' for the start of the month
        'W-MON' for weeks starting on Monday.
    """
    date_cols = [col for col in df.columns if "datum" in col or "date" in col]

    counts_dfs = []

    for col in date_cols:
        if len(df[~df[col].isna()]):
            count_df = df[['project', col]].groupby(by=[pd.Grouper(key=col,
                                                                   freq=freq,
                                                                   closed='left',
                                                                   label="left")
                                                        ]
                                                    ).count()
            count_df = count_df.reset_index().rename(columns={col: "date", "project": f"count_{col}"})
            counts_dfs.append(count_df)
        else:
            counts_dfs.append(pd.DataFrame(columns=["date", f"count_{col}"]))

    df_merged = reduce(lambda left, right: pd.merge(left, right, on=['date'],
                                                    how='outer'), counts_dfs)
    df_merged['date'] = df_merged['date'].dt.strftime('%Y-%m-%d')
    df_merged = df_merged.sort_values(by=["date"]).set_index('date')

    record = {col: pd.Series(series).dropna().to_dict() for col, series in df_merged.iteritems()}
    return record


def slice_for_jaaroverzicht(data):
    df = pd.DataFrame.from_dict(data, orient='index', columns=['count_by_month'])
    df.index = pd.to_datetime(df.index)
    period = ['2019-12-23', '2020-12-27']
    return df.loc[period[0]:period[1], 'count_by_month'].to_list()


# def preprocess_for_jaaroverzicht(*args):
#     return [slice_for_jaaroverzicht(arg) for arg in args]


# def calculate_jaaroverzicht(realisatie, planning, HAS_werkvoorraad, HC_HPend, on_time_ratio, outlook, bis_gereed):
#     realisatie_sum = round(sum(realisatie))
#     planning_sum = sum(planning)
#     planning_result = planning_sum - realisatie_sum
#     jaaroverzicht = dict(id='jaaroverzicht',
#                          real=str(realisatie_sum),
#                          plan=str(planning_result),
#                          HAS_werkvoorraad=str(HAS_werkvoorraad),
#                          ratio_op_tijd="{:.2f}".format(on_time_ratio),
#                          bis_gereed=str(bis_gereed),
#                          prog_c='pretty_container',
#                          target=str(outlook))
#
#     return jaaroverzicht
