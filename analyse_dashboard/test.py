# %% Initialize
import os
import time
import analyse.config as config
from analyse.functions import get_data_FC, get_data_planning, get_data_targets
from analyse.functions import targets, prognose, overview, calculate_projectspecs, calculate_y_voorraad_act
from analyse.functions import set_filters, prognose_graph, performance_matrix, info_table, set_bar_names
from analyse.functions import graph_overview, masks_phases, map_redenen, consume, analyse_to_firestore

# %% Set environment variables and permissions and data path
keys = os.listdir(config.path_jsons)
for fn in keys:
    if ('-d-' in fn) & ('-fttx-' in fn):
        gpath_d = config.path_jsons + fn
    if ('-p-' in fn) & ('-fttx-' in fn):
        gpath_p = config.path_jsons + fn
    if ('-d-' in fn) & ('-it-fiber' in fn):
        gpath_i = config.path_jsons + fn

# %% Get data from state collection Projects
t_start = time.time()
df_l, t_s, x_d, tot_l = get_data_FC(config.subset_KPN_2020, config.col, gpath_i, config.path_data)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gpath_d
# df_l, t_s, x_d, tot_l = get_data_FC(config.subset_KPN_2020, config.col, None, None)
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
print('write to Graph collection: ' + str((time.time() - t_start) / 60) + ' min')
consume(df_l)
print('write to Graph collection: ' + str((time.time() - t_start) / 60) + ' min')


# hoe zit het met tot l lege projecten?
df_l_t = {}
for key in df_l:
    if df_l[key].empty:
        print(key)
        # df_l[key] = df_l_t[key]
