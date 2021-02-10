# %%
import logging
import argparse
import os

import config
from Analyse.KPNDFN import DFNLocalETL, KPNETL, DFNETL
from Analyse.KPNDFN import KPNLocalETL
from Analyse.TMobile import TMobileLocalETL, TMobileETL
from builtins import input

logging.basicConfig(
    format=' %(asctime)s - %(name)s -%(levelname)s - %(filename)s:%(funcName)s:%(lineno)s - %(message)s',
    level=logging.INFO
)


def run_client(client_name, etl_process, steps=None, reload=False, project=None):
    etl = etl_process(client=client_name, config=config.client_config[client_name])
    if steps is None:
        etl.perform()
    else:
        step_list = [etl.extract, etl.transform, etl.analyse, etl.load]
        [step() for step in step_list[:steps]]


def get_etl_process(client, local=True):
    if local:
        type = 'local'
    else:
        type = 'real'
    etl_processes = {
                    'kpn': {
                            'local': KPNLocalETL,
                            'real': KPNETL,
                            },
                    'tmobile': {
                                'local': TMobileLocalETL,
                                'real': TMobileETL,
                               },
                    'dfn': {
                            'local': DFNLocalETL,
                            'real': DFNETL,
                           }
                    }
    return etl_processes[client][type]


def set_config_project(project, client):
    if client == 'kpn' and project in config.subset_KPN_2021:
        config.subset_KPN_2021 = [project]
    elif client == 'tmobile' and project in config.projects_tmobile:
        config.projects_tmobile = [project]
    elif client == 'dfn' and project in config.projects_dfn:
        config.projects_dfn = [project]
    else:
        raise NotImplementedError("Please select a correct combination of project and client")


def remove_pickle(client):
    filename = f'{client}_data.pickle'
    if os.path.exists(filename):
        os.remove(filename)
        print("Old pickle has been removed.")
    else:
        print("No pickle was present.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--client', help='Run the analysis for a specific client')
    parser.add_argument('--local',
                        help='Write results to local firestore or actual firestore',
                        default=True)
    parser.add_argument('--project', help='Run the analysis only for a specific project')
    parser.add_argument('--force-reload', help="Force reloading data from database, rather than using local pickle.")
    args = parser.parse_args()
    local = args.local
    project = args.project
    client = args.client
    set_config_project(project, client)

    if not local:
        prompt = input("Data will be written to the development firestore, confirm that this is intentional (y/n):")
        if prompt != 'y':
            raise ValueError("Please run with --local is True (the default value) to write to the local firestore.")
    if client:
        clients = [client]
    else:
        clients = ['kpn', 'tmobile', 'dfn']

    for client in clients:
        etl_process = get_etl_process(client=client, local=local)
        if args.force_reload:
            remove_pickle(client)
            run_client(client, etl_process, steps=1)
        run_client(client, etl_process)


# if __name__ == "__main__":
#     if 'FIRESTORE_EMULATOR_HOST' in os.environ:
#         logging.info('writing to local firestore')
#         client_name = "kpn"
#         kpn = KPNLocalETL(client=client_name, config=config.client_config[client_name])
#         kpn.perform()
#         client_name = "tmobile"
#         tmobile = TMobileLocalETL(client=client_name, config=config.client_config[client_name])
#         tmobile.perform()
#         client_name = "dfn"
#         dfn = DFNLocalETL(client=client_name, config=config.client_config[client_name])
#         dfn.perform()
#     else:
#         logging.info('testing ETL, not writing to firestore')
#         client_name = "kpn"
#         kpn = KPNTestETL(client=client_name, config=config.client_config[client_name])
#         kpn.perform()
#         client_name = "tmobile"
#         tmobile = TMobileTestETL(client=client_name, config=config.client_config[client_name])
#         tmobile.perform()
#         client_name = "dfn"
#         dfn = DFNTestETL(client=client_name, config=config.client_config[client_name])
#         dfn.perform()

# %%

# %%
