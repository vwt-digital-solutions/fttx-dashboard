# %% Initialize
import os
import time
import analyse.config as config
import numpy as np
from analyse.functions import get_data_FC, get_data_planning, get_data_targets
from analyse.functions import targets, prognose, overview, calculate_projectspecs, calculate_y_voorraad_act
from analyse.functions import set_filters, prognose_graph, performance_matrix, info_table, set_bar_names
from analyse.functions import graph_overview, masks_phases, map_redenen, consume, analyse_to_firestore, set_date_update

# %% Set environment variables and permissions and data path
keys = os.listdir(config.path_jsons)
print(keys)
for fn in keys:
    if ('-d-' in fn) & ('-fttx-' in fn):
        gpath_d = config.path_jsons + fn
    if ('-p-' in fn) & ('-fttx-' in fn):
        gpath_p = config.path_jsons + fn
    if ('-d-' in fn) & ('-it-fiber' in fn):
        gpath_i = config.path_jsons + fn

# %% Get data from state collection Projects
t_start = time.time()
df_l, t_s, x_d, tot_l = get_data_FC(config.subset_KPN_2020, config.col, gpath_i, config.path_data, 0)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gpath_d
# df_l, t_s, x_d, tot_l = get_data_FC(config.subset_KPN_2020, config.col, None, None, 0)
HP = get_data_planning(config.path_data, config.subset_KPN_2020)
# date_FTU0, date_FTU1 = get_data_targets(config.path_data)  # if path_data is None, then FTU from firestore
date_FTU0, date_FTU1 = get_data_targets(None)  # if path_data is None, then FTU from firestore
print('get data: ' + str((time.time() - t_start) / 60) + ' min')

# %% Analysis
t_start = time.time()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gpath_d
HC_HPend, HC_HPend_l, Schouw_BIS, HPend_l = calculate_projectspecs(df_l, '2020')
y_voorraad_act = calculate_y_voorraad_act(df_l)
rc1, rc2, d_real_l, y_prog_l, x_prog, t_shift, cutoff = prognose(df_l, t_s, x_d, tot_l, date_FTU0)
y_target_l, t_diff = targets(x_prog, x_d, t_shift, date_FTU0, date_FTU1, rc1, d_real_l)
df_prog, df_target, df_real, df_plan = overview(x_d, y_prog_l, tot_l, d_real_l, HP, y_target_l)
# write analysis result to Graphs collection
analyse_to_firestore(date_FTU0, date_FTU1, y_target_l, rc1, x_prog, x_d, d_real_l, df_prog, df_target, df_real,
                     df_plan, HC_HPend, y_prog_l, tot_l, HP, t_shift, rc2, cutoff, y_voorraad_act, HC_HPend_l, Schouw_BIS, HPend_l)

print('do analyses: ' + str((time.time() - t_start) / 60) + ' min')

# %% to fill collection Graphs
t_start = time.time()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gpath_d
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gpath_p

set_filters(df_l)
map_redenen()
# add_token_mapbox(config.mapbox_token)
graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, res='W-MON')  # 2019-12-30 -- 2020-12-21
graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, res='M')  # 2019-12-30 -- 2020-12-21
performance_matrix(x_d, y_target_l, d_real_l, tot_l, t_diff, y_voorraad_act)
prognose_graph(x_d, y_prog_l, d_real_l, y_target_l)
info_table(tot_l, d_real_l, HP, y_target_l, x_d, HC_HPend_l, Schouw_BIS, HPend_l)
print('write to Graph collection: ' + str((time.time() - t_start) / 60) + ' min')
t_start = time.time()
for i, pkey in enumerate(config.subset_KPN_2020):
    # df_l = get_data_projects([pkey], config.col)
    bar_m = masks_phases(pkey, df_l)
    print(i)
set_bar_names(bar_m)
set_date_update()
print('write to Graph collection: ' + str((time.time() - t_start) / 60) + ' min')
consume(df_l)
print('write to Graph collection: ' + str((time.time() - t_start) / 60) + ' min')

# %% Extra tests

# jsons vs state database
df_l, _, _, _ = get_data_FC(config.subset_KPN_2020, config.col, gpath_i, config.path_data, 0)
# df_l_r, _, _, _ = get_data_FC(config.subset_KPN_2020, config.col, gpath_i, config.path_data, 0)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gpath_d
df_l_r, _, _, _ = get_data_FC(config.subset_KPN_2020, config.col, None, None, 0)

for key in df_l:
    print(key + ': ' + str(df_l_r[key].shape[0] - df_l[key].shape[0]))
    print(key + ': ' + str(len(df_l[key].append(df_l_r[key], ignore_index=True).drop_duplicates(keep=False))))

key = 'KPN Gouda Kort Haarlem en Noord'
test = df_l[key].append(df_l_r[key], ignore_index=True)
uni = test.drop_duplicates(keep=False)
mask = test[test.duplicated()].iloc[0].sleutel
t1 = df_l[key][df_l[key].sleutel == mask]
t2 = df_l_r[key][df_l_r[key].sleutel == mask]

# hoe zit het met tot l lege projecten?
df_l_t = {}
for key in df_l:
    if df_l[key].empty:
        print(key)
        # df_l[key] = df_l_t[key]

# error check FC en BC
df_l, t_s, x_d, tot_l = get_data_FC(config.subset_KPN_2020, config.col, gpath_i, config.path_data, 1)
key = 'Bavel'
df = df_l[key]
errors_FC_BC = {key: {}}
errors_FC_BC[key]['101'] = df[df.KabelID.isna() & ~df.opleverdatum.isna() & (df.Postcode.isna() | df.Huisnummer.isna())].shape[0]
errors_FC_BC[key]['102'] = df[df.Plandatum.isna()].shape[0]
errors_FC_BC[key]['103'] = df[df.opleverdatum.isna() & df.opleverstatus.isin(['2', '10', '90', '91', '96', '97', '98', '99'])].shape[0]
errors_FC_BC[key]['104'] = df[df.opleverstatus.isna()].shape[0]
errors_FC_BC[key]['114'] = df[df.toestemming.isna()].shape[0]
errors_FC_BC[key]['115'] = errors_FC_BC[key]['118'] = df[df.soort_bouw.isna()].shape[0]  # zelfde error? gebouwtype vs type bouw?
errors_FC_BC[key]['116'] = df[df.FTU_type.isna()].shape[0]
errors_FC_BC[key]['117'] = df[df['Toelichting status'].isna() & df.opleverstatus.isin(['4', '12'])].shape[0]
errors_FC_BC[key]['119'] = df[df['Toelichting status'].isna() & df.redenna.isin(['R8', 'R9', 'R17'])].shape[0]

errors_FC_BC[key]['120'] = 0  # doorvoerafhankelijk niet aanwezig
errors_FC_BC[key]['121'] = df[(df.Postcode.isna() & ~df.Huisnummer.isna()) | (~df.Postcode.isna() & df.Huisnummer.isna())].shape[0]
errors_FC_BC[key]['122'] = df[~((df.Kast.isna() & df.Kastrij.isna() & df.ODFpos.isna() &  # kloppen deze velden?
                                 df.CATVpos.isna() & df.ODF.isna()) |
                                (~df.Kast.isna() & ~df.Kastrij.isna() & ~df.ODFpos.isna() &
                                 ~df.CATVpos.isna() & ~df.Areapop.isna() & ~df.ODF.isna()))].shape[0]
errors_FC_BC[key]['123'] = df[df.ProjectCode.isna()].shape[0]
errors_FC_BC[key]['301'] = df[~df.opleverdatum.isna() & df.opleverstatus.isin(['0', '14'])].shape[0]
errors_FC_BC[key]['303'] = df[df.KabelID.isna() & (df.Postcode.isna() | df.Huisnummer.isna())].shape[0]
errors_FC_BC[key]['304'] = 0  # geen column Kavel...
errors_FC_BC[key]['306'] = df[~df.KabelID.isna() & df.opleverstatus.isin(['90', '91', '96', '97', '98', '99'])].shape[0]
errors_FC_BC[key]['308'] = 0  # geen HLopleverdatum...
errors_FC_BC[key]['309'] = 0  # geen doorvoerafhankelijk aanwezig...

errors_FC_BC[key]['310'] = 0  # df[~df.KabelID.isna() & df.Areapop.isna()].shape[0]  # strengID != KabelID?
errors_FC_BC[key]['311'] = df[df.redenna.isna() & ~df.opleverstatus.isin(['2', '10', '50'])].shape[0]
errors_FC_BC[key]['501'] = sum([1 for el in df.Postcode if (len(el) != 6) | (not el[0:4].isnumeric()) |
                                                           (el[4].isnumeric()) | (el[5].isnumeric())])
errors_FC_BC[key]['502'] = 0  # niet te checken, geen toegang tot CLR
errors_FC_BC[key]['503'] = 0  # date is already present in different format...yyyy-mm-dd??
errors_FC_BC[key]['504'] = 0  # date is already present in different format...yyyy-mm-dd??
errors_FC_BC[key]['506'] = df[~df.opleverstatus.isin(['0', '1', '2', '4', '5', '6', '7,' '8', '9', '10', '11', '12', '13',
                                                      '14', '15', '30', '31', '33', '34', '35', '50', '90', '91', '96',
                                                      '97', '98', '99'])].shape[0]
errors_FC_BC[key]['508'] = 0  # niet te checken, geen toegang tot Areapop
errors_FC_BC[key]['509'] = sum([1 for el in df.Kastrij if (len(el) > 2) | (len(el) < 1) | (not el.isnumeric())])
errors_FC_BC[key]['510'] = sum([1 for el in df.Kast if (len(el) > 4) | (len(el) < 1) | (not el.isnumeric())])

errors_FC_BC[key]['511'] = sum([1 for el in df.ODF if (len(el) > 5) | (len(el) < 1) | (not el.isnumeric())])
errors_FC_BC[key]['512'] = sum([1 for el in df.ODFpos if (len(el) > 2) | (len(el) < 1) | (not el.isnumeric())])
errors_FC_BC[key]['513'] = sum([1 for el in df.CATV if (len(el) > 5) | (len(el) < 1) | (not el.isnumeric())])
errors_FC_BC[key]['514'] = sum([1 for el in df.CATVpos if (len(el) > 3) | (len(el) < 1) |
                                                          (not el.isnumeric())])  # drie posities, 999, zie err711
errors_FC_BC[key]['516'] = sum([1 for el in df.ProjectCode if (not str(el).isnumeric())])  # cannot check if projectcode is valid...
errors_FC_BC[key]['517'] = 0  # date is already present in different format...yyyy-mm-dd??
errors_FC_BC[key]['518'] = df[~df.toestemming.isin(['Ja', 'Nee', np.nan])].shape[0]
errors_FC_BC[key]['519'] = df[~df.soort_bouw.isin(['Laag', 'Hoog', 'Duplex', 'Woonboot', 'Onbekend'])].shape[0]
errors_FC_BC[key]['520'] = df[(df.FTU_type.isna() & df.opleverstatus.isin(['2', '10'])) |
                              (~df.FTU_type.isin(['FTU_GN01', 'FTU_GN02', 'FTU_PF01', 'FTU_PF02',
                                                  'FTU_TY01', 'FTU_ZS_GN01', 'FTU_TK01', 'Onbekend']))].shape[0]
errors_FC_BC[key]['521'] = sum([1 for el in df[~df['Toelichting status'].isna()]['Toelichting status'] if len(el) < 3])

errors_FC_BC[key]['522'] = 0  # Civieldatum not present in our FC dump
errors_FC_BC[key]['524'] = 0  # Kavel not present in our FC dump
errors_FC_BC[key]['527'] = 0  # HL opleverdatum not present in our FC dump
errors_FC_BC[key]['528'] = df[~df.redenna.isin([np.nan, 'R0', 'R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'R9',
                                                'R10', 'R11', 'R12', 'R13', 'R14', 'R15', 'R16', 'R17', 'R18', 'R19',
                                                'R20', 'R21', 'R22'])].shape[0]
errors_FC_BC[key]['531'] = 0  # strengID niet aanwezig in deze FCdump
errors_FC_BC[key]['532'] = sum([1 for el1, el2 in zip(df.ODFpos, df.CATVpos) if ((int(el2) - int(el1) != 1) & (int(el2) != 999)) |
                                                                                (int(el1) % 2 == 0)])
errors_FC_BC[key]['533'] = 0  # Doorvoerafhankelijkheid niet aanwezig in deze FCdump
errors_FC_BC[key]['534'] = 0  # geen toegang tot CLR om te kunnen checken
errors_FC_BC[key]['535'] = sum([1 for el in df[~df['Toelichting status'].isna()]['Toelichting status'] if ',' in el])
errors_FC_BC[key]['536'] = sum([1 for el in df[~df.KabelID.isna()].KabelID if len(el) < 3])

errors_FC_BC[key]['537'] = 0  # Blok not present in our FC dump
errors_FC_BC[key]['701'] = 0  # Kan niet gecheckt worden, hebben we vorige waarde voor nodig...
errors_FC_BC[key]['702'] = df[~df.ODF.isna() & df.opleverstatus.isin(['90', '91', '96', '97', '98', '99'])].shape[0]
errors_FC_BC[key]['707'] = 0  # Kan niet gecheckt worden, hebben we vorige waarde voor nodig...
errors_FC_BC[key]['708'] = df[(df.opleverstatus.isin(['90']) & ~df.redenna.isin(['R15', 'R16', 'R17'])) |
                              (df.opleverstatus.isin(['91']) & ~df.redenna.isin(['R12', 'R13', 'R14', 'R21']))].shape[0]
errors_FC_BC[key]['709'] = df[(df.ODF + df.ODFpos).duplicated()].shape[0]  # klopt deze aanname voor pop positie?
errors_FC_BC[key]['710'] = df[(df.KabelID + df.Adres).duplicated()].shape[0]  # klopt deze aanname voor pop positie?
