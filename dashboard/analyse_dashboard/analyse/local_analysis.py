# %%
import logging
import argparse
import os
import sys

import config
from Analyse.KPNDFN import DFNLocalETL, KPNETL, KPNLocalETL, DFNETL, KPNTestETL, DFNTestETL
from Analyse.TMobile import TMobileLocalETL, TMobileETL, TMobileTestETL

from builtins import input

logging.basicConfig(
    format=' %(asctime)s - %(name)s -%(levelname)s - %(filename)s:%(funcName)s:%(lineno)s - %(message)s',
    level=logging.INFO
)


def run_client(client_name, etl_process, steps=None):
    """
    Runs ETL-process for a specific client
    Args:
        client_name: name of client to be used in etl-process
        etl_process: Class of ETL-process to be ran.
        steps: Amount of steps to be taken, 1 through 4 for extract, transform, analysis and load respectively.
            Will run all steps upto the given number
    """
    etl = etl_process(client=client_name, config=config.client_config[client_name])
    if steps is None:
        print(f'Performing {etl_process.__name__} for {client_name}')
        etl.perform()
    else:
        step_list = [etl.extract, etl.transform, etl.analyse, etl.load]
        print(f"Performing {steps} steps for {etl_process.__name__}, client: {client_name}")
        [step() for step in step_list[:steps]]


def get_etl_process(client, etl_type='local'):
    """
    Retrieves ETL process given type and client
    Args:
        client:
        etl_type:

    Returns:

    """
    etl_processes = {
                    'kpn': {
                            'local': KPNLocalETL,
                            'write_to_dev': KPNETL,
                            'reload': KPNTestETL
                            },
                    'tmobile': {
                                'local': TMobileLocalETL,
                                'write_to_dev': TMobileETL,
                                'reload': TMobileTestETL
                               },
                    'dfn': {
                            'local': DFNLocalETL,
                            'write_to_dev': DFNETL,
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

    parser.add_argument('--local',
                        help='Write results to local firestore or actual firestore',
                        default=True)
    parser.add_argument('--project',
                        help='Run the analysis only for a specific project. Requires --client input.')
    parser.add_argument('--client',
                        choices=['kpn', 'dfn', 'tmobile'],
                        required='--project' in sys.argv,
                        help='Run the analysis for a specific client',
                        )
    parser.add_argument('--force-reload',
                        action='store_const',
                        const=True,
                        help="Force reloading data from database, rather than using local pickle.")
    parser.add_argument('--steps',
                        choices=['1', '2', '3', '4'],
                        default='4',
                        help=['Run the code only up to a certain point of the process: '
                              '1.Extract, 2.Transform, 3.Analyse, 4.Load (default)']
                        )
    args = parser.parse_args()
    local = args.local
    project = args.project
    client = args.client

    if client:
        clients = [client]
    else:
        clients = ['kpn', 'tmobile', 'dfn']

    if args.force_reload:
        if 'FIRESTORE_EMULATOR_HOST' in os.environ:
            del os.environ['FIRESTORE_EMULATOR_HOST']
        force_reload(clients)

    steps = int(args.steps)

    if project:
        if not client:
            raise ValueError("Please select a client in combination with a project")
        set_config_project(project, client)

    if local:
        os.environ['FIRESTORE_EMULATOR_HOST'] = 'localhost:8080'
        etl_type = 'local'
    else:
        prompt = input("Data will be written to the development firestore, confirm that this is intentional (y/n):")

        if prompt != 'y':
            raise ValueError("Please run with --local is True (the default value) to write to the local firestore.")
        etl_type = 'write_to_dev'

    for client in clients:
        etl_process = get_etl_process(client=client, etl_type=etl_type)
        run_client(client, etl_process, steps)
