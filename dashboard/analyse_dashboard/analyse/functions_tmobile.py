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
    We commonly use: \n
    -    'MS' for the start of the month
    -    'W-MON' for weeks starting on Monday.
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
