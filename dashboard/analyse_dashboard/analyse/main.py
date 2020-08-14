# %% Set data path
import config
import logging
import json
import base64
from gobits import Gobits
from google.cloud import pubsub, firestore
from functions import get_timeline, get_start_time, get_data, get_total_objects
from functions import preprocess_data, Analysis
from functions import get_data_FC, get_data_planning, get_data_targets
from functions import overview
from functions import set_filters, set_bar_names, error_check_FCBC
from functions import masks_phases, set_date_update


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

        # Get data from state collection Projects
        df_l = get_data(config.subset_KPN_2020, config.col, None, None, 0)
        start_time = get_start_time(df_l)
        timeline = get_timeline(start_time)
        total_objects = get_total_objects(df_l)
        HP = get_data_planning(config.path_data_b, config.subset_KPN_2020)
        date_FTU0, date_FTU1 = get_data_targets(None)  # if path_data is None, then FTU from firestore
        logging.info('data is retrieved')

        # Analysis
        analyse = Analysis('KPN')
        analyse.set_inputfields(date_FTU0, date_FTU1, timeline)
        df_l = preprocess_data(df_l, '2020')
        HC_HPend, HC_HPend_l, Schouw_BIS, HPend_l, HAS_werkvoorraad = analyse.calculate_projectspecs(df_l)
        y_voorraad_act = analyse.calculate_y_voorraad_act(df_l)
        rc1, rc2, d_real_l, y_prog_l, x_prog, t_shift, cutoff = analyse.prognose(df_l, start_time, timeline, total_objects, date_FTU0)
        y_target_l, t_diff = analyse.targets(x_prog, timeline, t_shift, date_FTU0, date_FTU1, rc1, d_real_l)
        df_prog, df_target, df_real, df_plan = overview(timeline, y_prog_l, total_objects, d_real_l, HP, y_target_l)
        n_err, errors_FC_BC = error_check_FCBC(df_l)

        # write analysis result to Graphs collection
        logging.info('analyses done')

        # to fill collection Graphs
        set_filters(df_l)

        analyse.calculate_graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad)  # 2019-12-30 -- 2020-12-21
        analyse.performance_matrix(timeline, y_target_l, d_real_l, total_objects, t_diff, y_voorraad_act)
        analyse.prognose_graph(timeline, y_prog_l, d_real_l, y_target_l)
        analyse.info_table(total_objects, d_real_l, HP, y_target_l, timeline, HC_HPend_l, Schouw_BIS, HPend_l, n_err)
        analyse.reden_na(df_l, config.clusters_reden_na)
        set_date_update()
        analyse.to_firestore()

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


def get_project_list():
    # We could get this list from the config file
    data = [
        el['label'] for el in firestore.Client().collection('Graphs').document('pnames').get().to_dict()['filters']
    ]
    return data


def publish_project_data(request):
    data = get_project_list()
    gobits = Gobits.from_request(request=request)
    i = 1
    for msg in data:
        publish_json(gobits, msg_data=msg, rowcount=i, rowmax=len(data), **config.TOPIC_SETTINGS)
        i += 1
