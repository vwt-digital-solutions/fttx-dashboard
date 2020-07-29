# %% Set data path
import config
import logging
import json
import base64
from gobits import Gobits
from google.cloud import pubsub, firestore
from functions import get_data_FC, get_data_planning, get_data_targets
from functions import targets, prognose, overview, calculate_projectspecs, calculate_y_voorraad_act
from functions import set_filters, prognose_graph, performance_matrix, info_table, set_bar_names, error_check_FCBC
from functions import graph_overview, masks_phases, map_redenen, analyse_to_firestore, set_date_update


logging.basicConfig(level=logging.INFO)
publisher = pubsub.PublisherClient()


def publish_json(gobits, msg_data, rowcount, rowmax, topic_project_id, topic_name, subject=None):
    topic_path = publisher.topic_path(topic_project_id, topic_name)
    if subject:
        msg = {
            "gobits": [gobits.to_json()],
            subject: msg_data
        }
    else:
        msg = msg_data
        logging.info(f'Publish to {topic_path}: {msg}')
    future = publisher.publish(
        topic_path, bytes(json.dumps(msg).encode('utf-8')))
    future.add_done_callback(
        lambda x: logging.debug(
            'Published msg with ID {} ({}/{} rows).'.format(
                future.result(), rowcount, rowmax))
    )


def analyse(request):
    try:
        data = [
            el['label'] for el in firestore.Client().collection('Graphs').document('pnames').get().to_dict()['filters']
        ]
        gobits = Gobits.from_request(request=request)
        i = 1
        for msg in data:
            publish_json(gobits, msg_data=msg, rowcount=i, rowmax=len(data), **config.TOPIC_SETTINGS)
            i += 1

        df_l, t_s, x_d, tot_l = get_data_FC(config.subset_KPN_2020, config.col, None, None, 0)
        # Get data from state collection Projects

        HP = get_data_planning(config.path_data_b, config.subset_KPN_2020)
        date_FTU0, date_FTU1 = get_data_targets(None)  # if path_data is None, then FTU from firestore
        logging.info('data is retrieved')

        # Analysis
        HC_HPend, HC_HPend_l, Schouw_BIS, HPend_l, Schouw, BIS = calculate_projectspecs(df_l, '2020')
        y_voorraad_act = calculate_y_voorraad_act(df_l)
        rc1, rc2, d_real_l, y_prog_l, x_prog, t_shift, cutoff = prognose(df_l, t_s, x_d, tot_l, date_FTU0)
        y_target_l, t_diff = targets(x_prog, x_d, t_shift, date_FTU0, date_FTU1, rc1, d_real_l)
        df_prog, df_target, df_real, df_plan = overview(x_d, y_prog_l, tot_l, d_real_l, HP, y_target_l)
        n_err, errors_FC_BC = error_check_FCBC(df_l)
        # write analysis result to Graphs collection
        analyse_to_firestore(date_FTU0, date_FTU1, y_target_l, rc1, x_prog, x_d, d_real_l, df_prog, df_target, df_real,
                             df_plan, HC_HPend, y_prog_l, tot_l, HP, t_shift, rc2, cutoff, y_voorraad_act, HC_HPend_l, Schouw_BIS,
                             HPend_l, Schouw, BIS, n_err)
        logging.info('analyses done')

        # to fill collection Graphs
        set_filters(df_l)
        map_redenen()
        graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, Schouw, BIS, res='W-MON')  # 2019-12-30 -- 2020-12-21
        graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, Schouw, BIS, res='M')  # 2019-12-30 -- 2020-12-21
        performance_matrix(x_d, y_target_l, d_real_l, tot_l, t_diff, y_voorraad_act)
        prognose_graph(x_d, y_prog_l, d_real_l, y_target_l)
        info_table(tot_l, d_real_l, HP, y_target_l, x_d, HC_HPend_l, Schouw_BIS, HPend_l, n_err)
        set_date_update()

        return 'OK', 204

    except Exception as e:
        logging.exception(f'Analyse failed {e}')
        return 'Error', 500

    finally:
        logging.info('run done')


def graph(request):
    try:
        envelope = json.loads(request.data.decode('utf-8'))
        bytes = base64.b64decode(envelope['message']['data'])
        project = json.loads(bytes)
        df_l, _, _, _ = get_data_FC([project], config.col, None, None, 0)
        bar_m = masks_phases(project, df_l)
        set_bar_names(bar_m)
        logging.info(f'masks bar uploaded for {project}')
    except Exception:
        logging.exception('Graph calculation failed')
