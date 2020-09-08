import pandas as pd
import numpy as np
from functions import get_hc_hpend_ratio_total, get_intersect


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


def get_hc_hpend_ratio_old(df_l):
    ratio_per_project = {}
    for project, data in df_l.items():
        try:
            ratio_per_project[project] = sum(data.homes_completed) / sum(data.hpend) * 100
        except ZeroDivisionError:
            # Dirty fix, check if it can be removed.
            ratio_per_project[project] = 0
    return ratio_per_project


def preprocess_data_old(df_l, year):
    df_l = add_relevant_columns_old(df_l, year)
    return df_l


def calculate_projectspecs_old(df_l):
    homes_completed = get_homes_completed_old(df_l)
    homes_ended = get_HPend_old(df_l)
    has_ready = get_has_ready_old(df_l)
    hc_hpend_ratio = get_hc_hpend_ratio_old(df_l)
    hc_hp_end_ratio_total = get_hc_hpend_ratio_total(homes_completed, homes_ended)
    werkvoorraad = get_has_werkvoorraad_old(df_l)

    return hc_hp_end_ratio_total, hc_hpend_ratio, has_ready, homes_ended, werkvoorraad


def prognose_old(df_l, t_s, x_d, tot_l, date_FTU0):
    x_prog = np.array(list(range(0, len(x_d))))
    cutoff = 85

    rc1 = {}
    rc2 = {}
    d_real_l = {}
    t_shift = {}
    y_prog_l = {}
    for key in df_l:  # to calculate prognoses for projects in FC
        d_real = df_l[key][~df_l[key]['opleverdatum'].isna()]
        if not d_real.empty:
            d_real = d_real.groupby(['opleverdatum']).agg({'sleutel': 'count'}).rename(columns={'sleutel': 'Aantal'})
            d_real.index = pd.to_datetime(d_real.index, format='%Y-%m-%d')
            d_real = d_real.sort_index()
            d_real = d_real[d_real.index < pd.Timestamp.now()]

            d_real = d_real.cumsum() / tot_l[key] * 100
            d_real[d_real.Aantal > 100] = 100  # only necessary for DH
            t_shift[key] = (d_real.index.min() - min(t_s.values())).days
            d_real.index = (d_real.index - d_real.index[0]).days + t_shift[key]
            d_real_l[key] = d_real

            d_rc1 = d_real[d_real.Aantal < cutoff]
            if len(d_rc1) > 1:
                rc1[key], b1 = np.polyfit(d_rc1.index, d_rc1, 1)
                y_prog1 = b1[0] + rc1[key][0] * x_prog
                y_prog_l[key] = y_prog1.copy()

            d_rc2 = d_real[d_real.Aantal >= cutoff]
            if (len(d_rc2) > 1) & (len(d_rc1) > 1):
                rc2[key], b2 = np.polyfit(d_rc2.index, d_rc2, 1)
                y_prog2 = b2[0] + rc2[key][0] * x_prog
                x_i, y_i = get_intersect([x_prog[0], y_prog1[0]], [x_prog[-1], y_prog1[-1]],
                                         [x_prog[0], y_prog2[0]], [x_prog[-1], y_prog2[-1]])
                y_prog_l[key][x_prog >= x_i] = y_prog2[x_prog >= x_i]
            # if (len(d_rc2) > 1) & (len(d_rc1) <= 1):
            #     rc1[key], b1 = np.polyfit(d_rc2.index, d_rc2, 1)
            #     y_prog1 = b1[0] + rc1[key][0] * x_prog
            #     y_prog_l[key] = y_prog1.copy()

    rc1_mean = sum(rc1.values()) / len(rc1.values())
    rc2_mean = sum(rc2.values()) / len(rc2.values())
    for key in df_l:
        if (key in rc1) & (key not in rc2):  # the case of 2 realisation dates, rc1 but no rc2
            if max(y_prog_l[key]) > cutoff:
                b2_mean = cutoff - (rc2_mean * x_prog[y_prog_l[key] >= cutoff][0])
                y_prog2 = b2_mean + rc2_mean * x_prog
                y_prog_l[key][y_prog_l[key] >= cutoff] = y_prog2[y_prog_l[key] >= cutoff]
        if (key in d_real_l) & (key not in y_prog_l):  # the case of only 1 realisation date
            b1_mean = -(rc1_mean * t_shift[key])
            y_prog1 = b1_mean + rc1_mean * x_prog
            b2_mean = cutoff - (rc2_mean * x_prog[y_prog1 >= cutoff][0])
            y_prog2 = b2_mean + rc2_mean * x_prog
            y_prog_l[key] = y_prog1.copy()
            y_prog_l[key][y_prog1 >= cutoff] = y_prog2[y_prog1 >= cutoff]
        if key not in d_real_l:  # the case of no realisation date
            t_shift[key] = x_prog[x_d == pd.Timestamp.now().strftime('%Y-%m-%d')][0]
            if key in date_FTU0:
                if not pd.isnull(date_FTU0[key]):
                    t_shift[key] = x_prog[x_d == date_FTU0[key]][0]
            b1_mean = -(rc1_mean * (t_shift[key] + 14))  # to include delay of two week
            y_prog1 = b1_mean + rc1_mean * x_prog
            b2_mean = cutoff - (rc2_mean * x_prog[y_prog1 >= cutoff][0])
            y_prog2 = b2_mean + rc2_mean * x_prog
            y_prog_l[key] = y_prog1.copy()
            y_prog_l[key][y_prog1 >= cutoff] = y_prog2[y_prog1 >= cutoff]

    for key in y_prog_l:
        y_prog_l[key][y_prog_l[key] > 100] = 100
        y_prog_l[key][y_prog_l[key] < 0] = 0

    return rc1, rc2, d_real_l, y_prog_l, x_prog, t_shift, cutoff


def set_filters_old(df_l):
    filters = []
    for key in df_l:
        filters += [{'label': key, 'value': key}]
    record = dict(filters=filters)
    return record


def error_check_FCBC_old(df_l):
    errors_FC_BC = {}
    for key in df_l:
        df = df_l[key]
        errors_FC_BC[key] = {}
        if not df.empty:
            errors_FC_BC[key]['101'] = df[df.kabelid.isna() & ~df.opleverdatum.isna() & (df.postcode.isna() |
                                                                                         df.huisnummer.isna())].sleutel.to_list()
            errors_FC_BC[key]['102'] = df[df.plandatum.isna()].sleutel.to_list()
            errors_FC_BC[key]['103'] = df[df.opleverdatum.isna() &
                                          df.opleverstatus.isin(
                                              ['2', '10', '90', '91', '96', '97', '98', '99'])].sleutel.to_list()
            errors_FC_BC[key]['104'] = df[df.opleverstatus.isna()].sleutel.to_list()
            # errors_FC_BC[key]['114'] = df[df.toestemming.isna()].sleutel.to_list()
            errors_FC_BC[key]['115'] = errors_FC_BC[key]['118'] = df[
                df.soort_bouw.isna()].sleutel.to_list()  # soort_bouw hoort bij?
            errors_FC_BC[key]['116'] = df[df.ftu_type.isna()].sleutel.to_list()
            errors_FC_BC[key]['117'] = df[
                df['toelichting_status'].isna() & df.opleverstatus.isin(['4', '12'])].sleutel.to_list()
            errors_FC_BC[key]['119'] = df[
                df['toelichting_status'].isna() & df.redenna.isin(['R8', 'R9', 'R17'])].sleutel.to_list()

            errors_FC_BC[key]['120'] = []  # doorvoerafhankelijk niet aanwezig
            errors_FC_BC[key]['121'] = df[(df.postcode.isna() & ~df.huisnummer.isna()) |
                                          (~df.postcode.isna() & df.huisnummer.isna())].sleutel.to_list()
            errors_FC_BC[key]['122'] = df[
                ~((df.kast.isna() & df.kastrij.isna() & df.odfpos.isna() &  # kloppen deze velden?
                   df.catvpos.isna() & df.odf.isna()) |
                  (~df.kast.isna() & ~df.kastrij.isna() & ~df.odfpos.isna() &
                   ~df.catvpos.isna() & ~df.areapop.isna() & ~df.odf.isna()))].sleutel.to_list()
            errors_FC_BC[key]['123'] = df[df.projectcode.isna()].sleutel.to_list()
            errors_FC_BC[key]['301'] = df[
                ~df.opleverdatum.isna() & df.opleverstatus.isin(['0', '14'])].sleutel.to_list()
            errors_FC_BC[key]['303'] = df[
                df.kabelid.isna() & (df.postcode.isna() | df.huisnummer.isna())].sleutel.to_list()
            errors_FC_BC[key]['304'] = []  # geen column Kavel...
            errors_FC_BC[key]['306'] = df[~df.kabelid.isna() &
                                          df.opleverstatus.isin(['90', '91', '96', '97', '98', '99'])].sleutel.to_list()
            errors_FC_BC[key]['308'] = []  # geen HLopleverdatum...
            errors_FC_BC[key]['309'] = []  # geen doorvoerafhankelijk aanwezig...

            errors_FC_BC[key][
                '310'] = []  # df[~df.KabelID.isna() & df.Areapop.isna()].sleutel.to_list()  # strengID != KabelID?
            errors_FC_BC[key]['311'] = df[
                df.redenna.isna() & ~df.opleverstatus.isin(['2', '10', '50'])].sleutel.to_list()
            errors_FC_BC[key]['501'] = [df.sleutel[el] for el in df[~df.postcode.isna()].index if
                                        (len(df.postcode[el]) != 6) |
                                        (not df.postcode[el][0:4].isnumeric()) |
                                        (df.postcode[el][4].isnumeric()) |
                                        (df.postcode[el][5].isnumeric())]
            errors_FC_BC[key]['502'] = []  # niet te checken, geen toegang tot CLR
            errors_FC_BC[key]['503'] = []  # date is already present in different format...yyyy-mm-dd??
            errors_FC_BC[key]['504'] = []  # date is already present in different format...yyyy-mm-dd??
            errors_FC_BC[key]['506'] = df[
                ~df.opleverstatus.isin(['0', '1', '2', '4', '5', '6', '7,' '8', '9', '10', '11', '12', '13',
                                        '14', '15', '30', '31', '33', '34', '35', '50', '90', '91', '96',
                                        '97', '98', '99'])].sleutel.to_list()
            errors_FC_BC[key]['508'] = []  # niet te checken, geen toegang tot Areapop
            errors_FC_BC[key]['509'] = [df.sleutel[el] for el in df[~df.kastrij.isna()].index if
                                        (len(df.kastrij[el]) > 2) |
                                        (len(df.kastrij[el]) < 1) |
                                        (not df.kastrij[el].isnumeric())]
            errors_FC_BC[key]['510'] = [df.sleutel[el] for el in df[~df.kast.isna()].index if (len(df.kast[el]) > 4) |
                                        (len(df.kast[el]) < 1) |
                                        (not df.kast[el].isnumeric())]

            errors_FC_BC[key]['511'] = [df.sleutel[el] for el in df[~df.odf.isna()].index if (len(df.odf[el]) > 5) |
                                        (len(df.odf[el]) < 1) |
                                        (not df.odf[el].isnumeric())]
            errors_FC_BC[key]['512'] = [df.sleutel[el] for el in df[~df.odfpos.isna()].index if
                                        (len(df.odfpos[el]) > 2) |
                                        (len(df.odfpos[el]) < 1) |
                                        (not df.odfpos[el].isnumeric())]
            errors_FC_BC[key]['513'] = [df.sleutel[el] for el in df[~df.catv.isna()].index if (len(df.catv[el]) > 5) |
                                        (len(df.catv[el]) < 1) |
                                        (not df.catv[el].isnumeric())]
            errors_FC_BC[key]['514'] = [df.sleutel[el] for el in df[~df.catvpos.isna()].index if
                                        (len(df.catvpos[el]) > 3) |
                                        (len(df.catvpos[el]) < 1) |
                                        (not df.catvpos[el].isnumeric())]
            errors_FC_BC[key]['516'] = [df.sleutel[el] for el in df[df.projectcode.isna()].index
                                        if (not str(df.projectcode[el]).isnumeric()) & (
                                            ~pd.isnull(df.projectcode[el]))]  # cannot check
            errors_FC_BC[key]['517'] = []  # date is already present in different format...yyyy-mm-dd??
            errors_FC_BC[key]['518'] = df[~df.toestemming.isin(['Ja', 'Nee', np.nan])].sleutel.to_list()
            errors_FC_BC[key]['519'] = df[
                ~df.soort_bouw.isin(['Laag', 'Hoog', 'Duplex', 'Woonboot', 'Onbekend'])].sleutel.to_list()
            errors_FC_BC[key]['520'] = df[(df.ftu_type.isna() & df.opleverstatus.isin(['2', '10'])) |
                                          (~df.ftu_type.isin(['FTU_GN01', 'FTU_GN02', 'FTU_PF01', 'FTU_PF02',
                                                              'FTU_TY01', 'FTU_ZS_GN01', 'FTU_TK01',
                                                              'Onbekend']))].sleutel.to_list()
            errors_FC_BC[key]['521'] = [df.sleutel[el] for el in
                                        df[~df['toelichting_status'].isna()]['toelichting_status'].index
                                        if len(df[~df['toelichting_status'].isna()]['toelichting_status'][el]) < 3]

            errors_FC_BC[key]['522'] = []  # Civieldatum not present in our FC dump
            errors_FC_BC[key]['524'] = []  # Kavel not present in our FC dump
            errors_FC_BC[key]['527'] = []  # HL opleverdatum not present in our FC dump
            errors_FC_BC[key]['528'] = df[
                ~df.redenna.isin([np.nan, 'R0', 'R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'R9',
                                  'R10', 'R11', 'R12', 'R13', 'R14', 'R15', 'R16', 'R17', 'R18', 'R19',
                                  'R20', 'R21', 'R22'])].sleutel.to_list()
            errors_FC_BC[key]['531'] = []  # strengID niet aanwezig in deze FCdump
            # if df[~df.CATVpos.isin(['999'])].shape[0] > 0:
            #     errors_FC_BC[key]['532'] = [df.sleutel[el] for el in df.ODFpos.index
            #                                 if ((int(df.CATVpos[el]) - int(df.ODFpos[el]) != 1) &
            #                                     (int(df.CATVpos[el]) != '999')) |
            #                                    (int(df.ODFpos[el]) % 2 == [])]
            errors_FC_BC[key]['533'] = []  # Doorvoerafhankelijkheid niet aanwezig in deze FCdump
            errors_FC_BC[key]['534'] = []  # geen toegang tot CLR om te kunnen checken
            errors_FC_BC[key]['535'] = [df.sleutel[el] for el in
                                        df[~df['toelichting_status'].isna()]['toelichting_status'].index
                                        if ',' in df['toelichting_status'][el]]
            errors_FC_BC[key]['536'] = [df.sleutel[el] for el in df[~df.kabelid.isna()].kabelid.index if
                                        len(df.kabelid[el]) < 3]

            errors_FC_BC[key]['537'] = []  # Blok not present in our FC dump
            errors_FC_BC[key]['701'] = []  # Kan niet gecheckt worden, hebben we vorige waarde voor nodig...
            errors_FC_BC[key]['702'] = df[
                ~df.odf.isna() & df.opleverstatus.isin(['90', '91', '96', '97', '98', '99'])].sleutel.to_list()
            errors_FC_BC[key]['707'] = []  # Kan niet gecheckt worden, hebben we vorige waarde voor nodig...
            errors_FC_BC[key]['708'] = df[(df.opleverstatus.isin(['90']) & ~df.redenna.isin(['R15', 'R16', 'R17'])) |
                                          (df.opleverstatus.isin(['91']) &
                                           ~df.redenna.isin(['R12', 'R13', 'R14', 'R21']))].sleutel.to_list()
            # errors_FC_BC[key]['709'] = df[(df.ODF + df.ODFpos).duplicated(keep='last')].sleutel.to_list()  # klopt dit?
            errors_FC_BC[key]['710'] = df[(df.kabelid + df.adres).duplicated()].sleutel.to_list()
            # errors_FC_BC[key]['711'] = df[~df.CATV.isin(['999']) | ~df.CATVpos.isin(['999'])].sleutel.to_list()  # wanneer PoP 999?
            errors_FC_BC[key]['713'] = []  # type bouw zit niet in onze FC dump
            # if df[df.ODF.isin(['999']) & df.ODFpos.isin(['999']) & df.CATVpos.isin(['999']) & df.CATVpos.isin(['999'])].shape[0] > 0:
            #     errors_FC_BC[key]['714'] = df[~df.ODF.isin(['999']) | ~df.ODFpos.isin(['999']) | ~df.CATVpos.isin(['999']) |
            #                                 ~df.CATVpos.isin(['999'])].sleutel.to_list()

            errors_FC_BC[key]['716'] = []  # niet te checken, geen toegang tot SIMA
            errors_FC_BC[key]['717'] = []  # type bouw zit niet in onze FC dump
            errors_FC_BC[key]['719'] = []  # kan alleen gecheckt worden met geschiedenis
            errors_FC_BC[key]['721'] = []  # niet te checken, geen Doorvoerafhankelijkheid in FC dump
            errors_FC_BC[key]['723'] = df[(df.redenna.isin(['R15', 'R16', 'R17']) & ~df.opleverstatus.isin(['90'])) |
                                          (df.redenna.isin(['R12', 'R12', 'R14', 'R21']) & ~df.opleverstatus.isin(
                                              ['91'])) |
                                          (df.opleverstatus.isin(['90']) & df.redenna.isin(
                                              ['R2', 'R11']))].sleutel.to_list()
            errors_FC_BC[key]['724'] = df[
                (~df.opleverdatum.isna() & df.redenna.isin(['R0', 'R19', 'R22']))].sleutel.to_list()
            errors_FC_BC[key]['725'] = []  # geen zicht op vraagbundelingsproject of niet
            errors_FC_BC[key]['726'] = []  # niet te checken, geen HLopleverdatum aanwezig
            errors_FC_BC[key]['727'] = df[df.opleverstatus.isin(['50'])].sleutel.to_list()
            errors_FC_BC[key]['728'] = []  # voorkennis nodig over poptype

            errors_FC_BC[key]['729'] = []  # kan niet checken, vorige staat FC voor nodig
            errors_FC_BC[key]['90x'] = []  # kan niet checken, extra info over bestand nodig!

    n_err = {}
    for key in errors_FC_BC:
        err_all = []
        for key2 in errors_FC_BC[key]:
            for el in errors_FC_BC[key][key2]:
                if el not in err_all:
                    err_all += [el]
        n_err[key] = len(err_all)

    return n_err, errors_FC_BC
