import pandas as pd


def column_to_datetime(input_column):
    input_column = pd.to_datetime(input_column, infer_datetime_format=True)
    return input_column


def add_weeknumber(input_column):
    input_column = input_column.dt.strftime('%V')
    return input_column


def has_maand_bar_chart(df):
    return df
