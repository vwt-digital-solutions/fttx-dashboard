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


def get_total_objects_old(df_l):  # Don't think this is necessary to calculate at this point, should be done later.
    total_objects = {k: len(v) for k, v in df_l.items()}
    # This hardcoded stuff can lead to unexpected behaviour. Should this still be in here?
    total_objects['Bergen op Zoom Noord  wijk 01 + Halsteren'] = 9.465  # not yet in FC, total from excel bouwstromen
    total_objects['Den Haag - Haagse Hout-Bezuidenhout West'] = 9.488  # not yet in FC, total from excel bouwstromen
    total_objects['Den Haag - Vrederust en Bouwlust'] = 11.918  # not yet in FC, total from excel bouwstromen
    return total_objects


def add_relevant_columns_old(df_l, year):
    def object_is_hpend(opleverdatum, year):
        # Will return TypeError if opleverdatum is nan, in which case object is not hpend
        try:
            is_hpend = (opleverdatum >= year + '-01-01') & (opleverdatum <= year + '-12-31')
        except TypeError:
            is_hpend = False
        return is_hpend

    for k, v in df_l.items():
        v['hpend'] = v.opleverdatum.apply(lambda x: object_is_hpend(x, '2020'))
        v['homes_completed'] = v.opleverstatus == '2'
        v['bis_gereed'] = v.opleverstatus != '0'
    return df_l


def get_homes_completed_old(df_l):
    return {k: sum(v.homes_completed) for k, v in df_l.items()}


def get_HPend_old(df_l):
    return {k: sum(v.hpend) for k, v in df_l.items()}


def get_has_ready_old(df_l):
    return {k: len(v[~v.toestemming.isna() & v.bis_gereed]) for k, v in df_l.items()}


def calculate_y_voorraad_act_old(df_l):
    y_voorraad_act = {}
    for key in df_l:
        y_voorraad_act[key] = len(df_l[key][(~df_l[key].toestemming.isna()) &
                                            (df_l[key].opleverstatus != '0') &
                                            (df_l[key].opleverdatum.isna())])

    return y_voorraad_act


def get_has_werkvoorraad_old(df_l):
    return sum(calculate_y_voorraad_act_old(df_l).values())
