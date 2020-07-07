# %% Set data path
import config
import logging
from functions import get_data_FC, get_data_planning, get_data_targets
from functions import targets, prognose, overview, calculate_projectspecs, calculate_y_voorraad_act
from functions import set_filters, prognose_graph, performance_matrix, info_table
from functions import graph_overview, masks_phases, map_redenen, analyse_to_firestore


def analyse(request):
    try:
        # Get data from state collection Projects
        df_l, t_s, x_d, tot_l = get_data_FC(config.subset_KPN_2020, config.col, None, None)
        HP = get_data_planning(config.path_data_b, config.subset_KPN_2020)
        date_FTU0, date_FTU1 = get_data_targets(None)
        logging.info('data is retrieved')

        # Analysis
        HC_HPend, HC_HPend_l, Schouw_BIS, HPend_l = calculate_projectspecs(df_l)
        y_voorraad_act = calculate_y_voorraad_act(df_l)
        rc1, rc2, d_real_l, y_prog_l, x_prog, t_shift, cutoff = prognose(df_l, t_s, x_d, tot_l, date_FTU0)
        y_target_l, t_diff = targets(x_prog, x_d, t_shift, date_FTU0, date_FTU1, rc1, d_real_l)
        df_prog, df_target, df_real, df_plan = overview(x_d, y_prog_l, tot_l, d_real_l, HP, y_target_l)
        analyse_to_firestore(date_FTU0, date_FTU1, y_target_l, rc1, x_prog, x_d, d_real_l, df_prog, df_target,
                             df_real, df_plan, HC_HPend, y_prog_l, tot_l, HP, t_shift, rc2, cutoff, y_voorraad_act,
                             HC_HPend_l, Schouw_BIS, HPend_l)
        logging.info('analyses done')

        # to fill collection Graphs
        set_filters(df_l)
        map_redenen()
        graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, res='W-MON')  # 2019-12-30 -- 2020-12-21
        graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, res='M')  # 2019-12-30 -- 2020-12-21
        performance_matrix(x_d, y_target_l, d_real_l, tot_l, t_diff, y_voorraad_act)
        prognose_graph(x_d, y_prog_l, d_real_l, y_target_l)
        info_table(tot_l, d_real_l, HP, y_target_l, x_d, HC_HPend_l, Schouw_BIS, HPend_l)
        logging.info('graphs uploaded')
        for pkey in config.subset_KPN_2020:
            _ = masks_phases(pkey, df_l)
        logging.info('masks bar uploaded')

        return 'OK', 204

    except Exception as e:
        logging.exception(f'Analyse failed {e}')
        return 'Error', 500

    finally:
        logging.info('run done')
