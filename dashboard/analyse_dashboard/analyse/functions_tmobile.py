try:
    from functions import pie_chart_reden_na, get_pie_layout
except ImportError:
    from analyse.functions import pie_chart_reden_na, get_pie_layout
import pandas as pd


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
        project_data = project_data[project_data.project == project]
        data, document = pie_chart_reden_na(project_data, clusters, project)
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


def has_maand_bar_chart(df):
    has_maand = df.loc[(df.hasdatum.dt.year >= 2020) & (df.hasdatum.notnull()), :]['hasdatum'].dt.month.to_list()
    has_maand.sort()
    has_maand = [str(x) for x in has_maand]
    has_maand_dict = dict()
    for i in has_maand:
        has_maand_dict[i] = has_maand_dict.get(i, 0) + 1

    return has_maand_dict


def has_week_bar_chart(df):
    this_week = int(df.datetime.today().strftime('%V'))
    five_weeks_ago = this_week - 5
    this_week = str(this_week)
    five_weeks_ago = str(five_weeks_ago)

    has_week = df.loc[(df.hasdatum.dt.year >= 2020) & (df.hasdatum.notnull()), :]['hasdatum_week'].to_list()
    has_week.sort()

    has_week = [i for i in has_week if (i > five_weeks_ago) & (i <= this_week)]
    has_week = [str(x) for x in has_week]

    has_week_dict = dict()
    for i in has_week:
        has_week_dict[i] = has_week_dict.get(i, 0) + 1

    return has_week_dict


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

    voorraad_project_counts = voorraad_project_counts.append(totals).set_index("project").to_dict()['voorraad_count']
    return voorraad_project_counts
