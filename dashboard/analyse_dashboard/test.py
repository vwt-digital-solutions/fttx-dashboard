# %% Initialize
import os
import time

from Record import ListRecord, DocumentListRecord

try:
    import analyse.config as config
    from analyse.functions import get_data_planning, get_data_targets, preprocess_data
    from analyse.functions import get_timeline, get_start_time, get_data, get_total_objects
    from analyse.functions import overview
    from analyse.functions import error_check_FCBC
    from analyse.functions import masks_phases, map_redenen, consume, set_date_update
    from analyse.Analysis import AnalysisKPN
except ImportError:
    # import config as config
    from functions import get_data_planning, get_data_targets, preprocess_data
    from functions import get_timeline, get_start_time, get_data, get_total_objects
    from functions import overview
    from functions import error_check_FCBC
    from functions import masks_phases, map_redenen, consume, set_date_update
    from Analysis import AnalysisKPN

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
# df_l, t_s, x_d, tot_l = get_data_FC(config.subset_KPN_2020, config.col, gpath_i, config.path_data, 0)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gpath_d
df_l = get_data(config.subset_KPN_2020, config.col, None, None, 0)
start_time = get_start_time(df_l)
timeline = get_timeline(start_time)
total_objects = get_total_objects(df_l)
HP = get_data_planning(config.path_data, config.subset_KPN_2020)
# date_FTU0, date_FTU1 = get_data_targets(config.path_data)  # if path_data is None, then FTU from firestore
date_FTU0, date_FTU1 = get_data_targets(None)  # if path_data is None, then FTU from firestore
print('get data: ' + str((time.time() - t_start) / 60) + ' min')

# %% Analysis
analyse = AnalysisKPN('KPN')
analyse.set_input_fields(date_FTU0, date_FTU1, timeline)
df_l = preprocess_data(df_l, '2020')
HC_HPend, HC_HPend_l, Schouw_BIS, HPend_l, HAS_werkvoorraad = analyse.calculate_projectspecs(df_l)
y_voorraad_act = analyse.calculate_y_voorraad_act(df_l)
rc1, rc2, d_real_l, y_prog_l, x_prog, t_shift, cutoff = analyse.prognose(df_l, start_time, timeline, total_objects, date_FTU0)
y_target_l, t_diff = analyse.targets(x_prog, timeline, t_shift, date_FTU0, date_FTU1, rc1, d_real_l)
df_prog, df_target, df_real, df_plan = overview(timeline, y_prog_l, total_objects, d_real_l, HP, y_target_l)
n_err, errors_FC_BC = error_check_FCBC(df_l)

print('do analyses: ' + str((time.time() - t_start) / 60) + ' min')

# %% to fill collection Graphs
t_start = time.time()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gpath_d
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gpath_p

analyse.set_filters(df_l)
map_redenen()
# add_token_mapbox(config.mapbox_token)
analyse.calculate_graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad)  # 2019-12-30 -- 2020-12-21
analyse.performance_matrix(timeline, y_target_l, d_real_l, total_objects, t_diff, y_voorraad_act)
analyse.prognose_graph(timeline, y_prog_l, d_real_l, y_target_l)
analyse.info_table(total_objects, d_real_l, HP, y_target_l, timeline, HC_HPend_l, Schouw_BIS, HPend_l, n_err)
analyse.reden_na(df_l, config.clusters_reden_na)
set_date_update()
analyse.to_firestore()
print('write to Graph collection: ' + str((time.time() - t_start) / 60) + ' min')
t_start = time.time()
total_document_list = []
bar_names = []
for i, pkey in enumerate(config.subset_KPN_2020):
    print(i, pkey)
    bar_names, document_list = masks_phases(pkey, df_l)
    total_document_list += document_list

dlr = DocumentListRecord(total_document_list, collection="Data")
dlr.to_firestore(client="KPN")

lr = ListRecord(dict(bar_names=bar_names), collection="Data")
lr.to_firestore(graph_name="bar_names", client="KPN")

set_date_update()
print('write to Graph collection: ' + str((time.time() - t_start) / 60) + ' min')
consume(df_l)
print('write to Graph collection: ' + str((time.time() - t_start) / 60) + ' min')

# %% Extra tests

# jsons vs state database
# df_l, _, _, _ = get_data_FC(config.subset_KPN_2020, config.col, gpath_i, config.path_data, 0)
# df_l_r, _, _, _ = get_data_FC(config.subset_KPN_2020, config.col, gpath_i, config.path_data, 0)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gpath_d
# df_l_r, _, _, _ = get_data_FC(config.subset_KPN_2020, config.col, None, None, 0)

# for key in df_l:
#    print(key + ': ' + str(df_l_r[key].shape[0] - df_l[key].shape[0]))
#    print(key + ': ' + str(len(df_l[key].append(df_l_r[key], ignore_index=True).drop_duplicates(keep=False))))

# key = 'KPN Gouda Kort Haarlem en Noord'
# test = df_l[key].append(df_l_r[key], ignore_index=True)
# uni = test.drop_duplicates(keep=False)
# mask = test[test.duplicated()].iloc[0].sleutel
# t1 = df_l[key][df_l[key].sleutel == mask]
# t2 = df_l_r[key][df_l_r[key].sleutel == mask]

# hoe zit het met tot l lege projecten?
# df_l_t = {}
# for key in df_l:
#    if df_l[key].empty:
#        print(key)
# df_l[key] = df_l_t[key]
