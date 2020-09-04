import pandas as pd


def get_start_time_old(df_l):
    # What does t_s stand for? Would prefer to use a descriptive variable name.
    t_s = {}
    for key in df_l:
        if df_l[key][~df_l[key].opleverdatum.isna()].empty:
            t_s[key] = pd.to_datetime(pd.Timestamp.now().strftime('%Y-%m-%d'))
        else:  # I'm not sure its desireable to hard-set dates like this. Might lead to unexpected behaviour.
            t_s[key] = pd.to_datetime(df_l[key]['opleverdatum']).min()
    return t_s
