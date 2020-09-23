from functools import reduce
import pandas as pd

try:
    from functions import pie_chart_reden_na, get_pie_layout
except ImportError:
    from analyse.functions import pie_chart_reden_na, get_pie_layout


# Small change to functions for KPN to work with dataframe instead of dict
# Should be generalised in later stage
def overview_reden_na_df(df, clusters):
    data, document = pie_chart_reden_na(df, clusters, 'overview')
    layout = get_pie_layout()
    fig = {
        'data': data,
        'layout': layout
    }
    record = dict(id=document, figure=fig)
    return record


# Small change to functions for KPN to work with dataframe instead of dict
# Should be generalised in later stage
def individual_reden_na_df(project_data, clusters):
    record_dict = {}
    for project in project_data.project.unique():
        df_proj = project_data[project_data.project == project].copy()
        data, document = pie_chart_reden_na(df_proj, clusters, project)
        layout = get_pie_layout()
        fig = {
            'data': data,
            'layout': layout
        }
        record = dict(id=document, figure=fig)
        record_dict[document] = record
    return record_dict


def column_to_datetime(input_column):
    input_column = pd.to_datetime(input_column, errors='coerce', infer_datetime_format=True)
    return input_column


def add_weeknumber(input_column):
    input_column = input_column.dt.strftime('%V')
    return input_column


# def has_maand_bar_chart(df):
#     has_maand = df.loc[(df.hasdatum.dt.year >= 2020) & (df.hasdatum.notnull()), :]['hasdatum'].dt.month.to_list()
#     has_maand.sort()
#     has_maand = [str(x) for x in has_maand]
#     has_maand_dict = dict()
#     for i in has_maand:
#         has_maand_dict[i] = has_maand_dict.get(i, 0) + 1
#
#     return has_maand_dict
#
#
# def has_week_bar_chart(df):
#     this_week = int(df.datetime.today().strftime('%V'))
#     five_weeks_ago = this_week - 5
#     this_week = str(this_week)
#     five_weeks_ago = str(five_weeks_ago)
#
#     has_week = df.loc[(df.hasdatum.dt.year >= 2020) & (df.hasdatum.notnull()), :]['hasdatum_week'].to_list()
#     has_week.sort()
#
#     has_week = [i for i in has_week if (i > five_weeks_ago) & (i <= this_week)]
#     has_week = [str(x) for x in has_week]
#
#     has_week_dict = dict()
#     for i in has_week:
#         has_week_dict[i] = has_week_dict.get(i, 0) + 1
#
#     return has_week_dict


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

    voorraad_project_counts = voorraad_project_counts.append(totals).set_index("project").to_dict()['voorraad_count'].copy()
    return voorraad_project_counts

# Doesnt look like it is used
# def calculate_planning_has_by_week(df):
#     count_plan = df[['project', 'plandatum']].groupby(by=[pd.Grouper(key='plandatum', freq='W-MON')]).count()
#     count_plan = count_plan.rename(columns={"project": "count_plan"}).reset_index()
#
#     count_has = df[['project', 'hasdatum']].groupby(by=[pd.Grouper(key='hasdatum', freq='W-MON')]).count()
#     count_has = count_has.rename(columns={"project": "count_has"}).reset_index()
#
#     merged_df = pd.merge(left=count_plan, right=count_has, left_on="plandatum", right_on="hasdatum", how="outer")
#     merged_df['date'] = merged_df.hasdatum.combine_first(merged_df.plandatum).dt.strftime("%Y-%m-%d")
#     merged_df.drop(['plandatum', 'hasdatum'], axis=1, inplace=True)
#     record = merged_df.set_index("date").to_dict()
#
#     return record
#
#
# def calculate_planning_has_by_month(df):
#     count_plan = df[['project', 'plandatum']].groupby(by=[pd.Grouper(key='plandatum', freq='M')]).count()
#     count_plan = count_plan.rename(columns={"project": "count_plan"}).reset_index()
#
#     count_has = df[['project', 'hasdatum']].groupby(by=[pd.Grouper(key='hasdatum', freq='M')]).count()
#     count_has = count_has.rename(columns={"project": "count_has"}).reset_index()
#
#     merged_df = pd.merge(left=count_plan, right=count_has, left_on="plandatum", right_on="hasdatum", how="outer")
#     merged_df['date'] = merged_df.hasdatum.combine_first(merged_df.plandatum).dt.strftime("%Y-%m-%d")
#     merged_df.drop(['plandatum', 'hasdatum'], axis=1, inplace=True)
#     record = merged_df.set_index("date").to_dict()
#
#     return record


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


def preprocess_for_jaaroverzicht(*args):
    return [slice_for_jaaroverzicht(arg) for arg in args]


def calculate_jaaroverzicht(realisatie, planning, HAS_werkvoorraad, HC_HPend):

    realisatie_sum = round(sum(realisatie))
    planning_sum = sum(planning)
    planning_result = planning_sum - realisatie_sum

    jaaroverzicht = dict(id='jaaroverzicht',
                         real=str(realisatie_sum),
                         plan=str(planning_result),
                         HC_HPend=str(HC_HPend),
                         HAS_werkvoorraad=str(HAS_werkvoorraad),
                         prog_c='pretty_container')

    return jaaroverzicht
