# %%
import logging
import argparse
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
    etl = etl_process(client_name=client_name, config=config.client_config[client_name])
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--client', help='Run the analysis for a specific client')
    parser.add_argument('--local',
                        help='Write results to local firestore or actual firestore',
                        default=True)
    args = parser.parse_args()
    local = args.local
    if local:
        prompt = input("Data will be written to the development firestore, confirm that this is intentional (y/n):")
        if prompt != 'y':
            raise ValueError("Please run with --local is True (the default value) to write to the local firestore.")
    if args.client:
        client = [args.client]
    else:
        clients = ['kpn', 'tmobile', 'dfn']

    for client in clients:
        etl_process = get_etl_process(client=client, local=local)
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
