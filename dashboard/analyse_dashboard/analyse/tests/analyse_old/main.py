# %% Set data path
import logging
import json
import base64

logging.basicConfig(level=logging.INFO)

try:
    from gobits import Gobits
    import config
    from tests.analyse_old.Customer import CustomerTmobile, CustomerKPN
    from functions import get_timeline, get_start_time, get_data
    from functions import preprocess_data, get_total_objects
    from functions import overview
    from functions import error_check_FCBC
    from functions import masks_phases, set_date_update
    from tests.analyse_old.Analysis import AnalysisKPN, AnalysisTmobile
    from google.cloud import pubsub, firestore
    from Analyse.Record import DocumentListRecord, ListRecord

    publisher = pubsub.PublisherClient()

except ImportError:
    import analyse.config as config
    from analyse.Customer import CustomerTmobile, CustomerKPN
    from analyse.functions import get_timeline, get_start_time, get_data
    from analyse.functions import preprocess_data, get_total_objects
    from analyse.functions import overview
    from analyse.functions import error_check_FCBC
    from analyse.functions import masks_phases, set_date_update
    from analyse.Analysis import AnalysisKPN, AnalysisTmobile


def analyse(request):
    try:
        analyseKPN('kpn')
        publish_project_data(request)
        analyseTmobile('t-mobile')
        set_date_update()
        return 'OK', 200

    except Exception as e:
        logging.exception(f'Analyse failed {e}')
        return 'Error', 500

    finally:
        logging.info('run done')


def analyseKPN(client_name):
    client_config = config.client_config[client_name]
    customer = CustomerKPN(client_config)
    df_l = customer.get_data()
    HP = customer.get_data_planning()
    date_FTU0, date_FTU1 = customer.get_data_targets()

    start_time = get_start_time(df_l)
    timeline = get_timeline(start_time)
    total_objects = get_total_objects(df_l)

    analyse = AnalysisKPN(client_name)
    analyse.set_input_fields(date_FTU0, date_FTU1, timeline)
    df_l = preprocess_data(df_l, '2020')
    analyse = kpn_analysis_variable_use(analyse, df_l, start_time, timeline, total_objects, HP, date_FTU0, date_FTU1)

    analyse.to_firestore()


def kpn_analysis_variable_use(analyse, df_l, start_time, timeline, total_objects, HP, date_FTU0, date_FTU1):
    HC_HPend, HC_HPend_l, Schouw_BIS, HPend_l, HAS_werkvoorraad = analyse.calculate_projectspecs(df_l)
    y_voorraad_act = analyse.calculate_y_voorraad_act(df_l)
    rc1, rc2, d_real_l, y_prog_l, x_prog, t_shift, cutoff = analyse.prognose(df_l, start_time, timeline, total_objects,
                                                                             date_FTU0)
    y_target_l, t_diff = analyse.targets(x_prog, timeline, t_shift, date_FTU0, date_FTU1, rc1, d_real_l)
    df_prog, df_target, df_real, df_plan = overview(timeline, y_prog_l, total_objects, d_real_l, HP, y_target_l)
    n_err, errors_FC_BC = error_check_FCBC(df_l)

    analyse.analyse_documents(date_FTU0, date_FTU1, y_target_l, rc1, x_prog, timeline, d_real_l, df_prog, df_target, df_real,
                              df_plan, HC_HPend, y_prog_l, total_objects, HP, t_shift, rc2, cutoff, y_voorraad_act,
                              HC_HPend_l,
                              Schouw_BIS, HPend_l, n_err, None, None)

    analyse.set_filters(df_l)
    analyse.calculate_graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend,
                                     HAS_werkvoorraad)  # 2019-12-30 -- 2020-12-21
    analyse.performance_matrix(timeline, y_target_l, d_real_l, total_objects, t_diff, y_voorraad_act)
    analyse.prognose_graph(timeline, y_prog_l, d_real_l, y_target_l)
    analyse.info_table(total_objects, d_real_l, HP, y_target_l, timeline, HC_HPend_l, Schouw_BIS, HPend_l, n_err)
    analyse.reden_na(df_l, config.clusters_reden_na)

    return analyse


def analyseTmobile(client_name):
    client_config = config.client_config[client_name]
    customer = CustomerTmobile(client_config)
    df_l = customer.get_data()

    analyse = AnalysisTmobile(client_name, df_l)
    analyse.reden_na(config.clusters_reden_na)
    analyse.get_voorraadvormend()

    analyse.to_firestore()


def graph(request):
    try:
        envelope = json.loads(request.data.decode('utf-8'))
        bytes = base64.b64decode(envelope['message']['data'])
        project = json.loads(bytes)
        df_l = get_data([project], config.col, None, None, 0)
        bar_names, document_list = masks_phases(project, df_l)
        dlr = DocumentListRecord(document_list, collection="Data", document_key=['filter', 'project'])
        dlr.to_firestore(client="KPN")

        lr = ListRecord(dict(bar_names=bar_names), collection="Data")
        lr.to_firestore(graph_name="bar_names", client="KPN")

        logging.info(f'masks bar uploaded for {project}')
    except Exception:
        logging.exception('Graph calculation failed')


def get_project_list():
    # We could get this list from the config file
    data = [
        el['label'] for el in firestore.Client().collection('Data').document('kpn_project_names').get().to_dict()['record']['filters']
    ]
    return data


def publish_project_data(request):
    data = get_project_list()
    gobits = Gobits.from_request(request=request)
    i = 1
    for msg in data:
        publish_json(gobits, msg_data=msg, rowcount=i, rowmax=len(data), **config.TOPIC_SETTINGS)
        i += 1


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
