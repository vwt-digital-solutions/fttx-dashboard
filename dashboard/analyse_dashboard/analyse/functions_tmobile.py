import pandas as pd


def column_to_datetime(input_column):
    input_column = pd.to_datetime(input_column, infer_datetime_format=True)
    return input_column


def add_weeknumber(input_column):
    input_column = input_column.dt.strftime('%V')
    return input_column


def has_maand_bar_chart(df):
    has_maand = df.loc[(df.HASdatum.dt.year >= 2020) & (df.HASdatum.notnull()), :]['HASdatum'].dt.month.to_list()
    has_maand.sort()
    has_maand_dict = dict()
    for i in has_maand:
        has_maand_dict[i] = has_maand_dict.get(i, 0) + 1

    data = {
        'labels': has_maand_dict.keys(),
        'values': has_maand_dict.values()
    }
    document = 'has_maand_bar_chart'
    return data, document
