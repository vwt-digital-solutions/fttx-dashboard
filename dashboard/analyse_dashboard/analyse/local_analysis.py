# %%
import logging
import argparse
import os

import config
from Analyse.KPNDFN import DFNLocalETL, KPNETL, KPNLocalETL, DFNETL, KPNTestETL, DFNTestETL
from Analyse.TMobile import TMobileLocalETL, TMobileETL, TMobileTestETL

from builtins import input

logging.basicConfig(
    format=' %(asctime)s - %(name)s -%(levelname)s - %(filename)s:%(funcName)s:%(lineno)s - %(message)s',
    level=logging.INFO
)


def run_client(client_name, etl_process, steps=None, reload=False, project=None):
    etl = etl_process(client=client_name, config=config.client_config[client_name])
    if steps is None:
        print(f'Performing {etl_process.__name__} for {client_name}')
        etl.perform()
    else:
        step_list = [etl.extract, etl.transform, etl.analyse, etl.load]
        print(f"Performing {steps} steps for {etl_process.__name__}, client: {client_name}")
        [step() for step in step_list[:steps]]


def get_etl_process(client, etl_type='local'):
    etl_processes = {
                    'kpn': {
                            'local': KPNLocalETL,
                            'real': KPNETL,
                            'reload': KPNTestETL
                            },
                    'tmobile': {
                                'local': TMobileLocalETL,
                                'real': TMobileETL,
                                'reload': TMobileTestETL
                               },
                    'dfn': {
                            'local': DFNLocalETL,
                            'real': DFNETL,
                            'reload': DFNTestETL
                           }
                    }
    return etl_processes[client][etl_type]


def set_config_project(project, client):
    if project in config.client_config[client]['projects']:
        config.client_config[client]['projects'] = [project]
    else:
        raise ValueError("Please select a valid combination of client and project")


def remove_pickle(client):
    filename = f'{client}_data.pickle'
    if os.path.exists(filename):
        os.remove(filename)
        print("Old pickle has been removed.")
    else:
        print("No pickle was present.")


def force_reload(clients):
    for client in clients:
        remove_pickle(client)
        extract_process = get_etl_process(client=client, etl_type='reload')
        run_client(client, extract_process, steps=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--client', help='Run the analysis for a specific client', default=False)
    parser.add_argument('--local',
                        help='Write results to local firestore or actual firestore',
                        default=True)
    parser.add_argument('--project', help='Run the analysis only for a specific project')
    parser.add_argument('--force-reload',
                        action='store_const',
                        const=True,
                        help="Force reloading data from database, rather than using local pickle.")
    args = parser.parse_args()
    local = args.local
    project = args.project
    client = args.client

    if project:
        if not client:
            raise ValueError("Please select a client in combination with a project")
        set_config_project(project, client)

    if not local:
        prompt = input("Data will be written to the development firestore, confirm that this is intentional (y/n):")
        if prompt != 'y':
            raise ValueError("Please run with --local is True (the default value) to write to the local firestore.")
        etl_type = 'real'

    if client:
        clients = [client]
    else:
        clients = ['kpn', 'tmobile', 'dfn']

    if args.force_reload:
        force_reload(clients)

    if local:
        os.environ['FIRESTORE_EMULATOR_HOST'] = 'localhost:8080'
        etl_type = 'local'

    for client in clients:
        etl_process = get_etl_process(client=client, etl_type=etl_type)
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
