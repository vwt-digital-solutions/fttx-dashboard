import pickle

from analyse_dashboard.analyse import config
from Analyse.KPN import KPNTestETL

import logging

from tests.analyse_old.ETL import ExtractTransformProjectDataFirestoreToDfList
from tests.analyse_old.main_to_test import analyseKPN

logging.basicConfig(format=' %(asctime)s - %(levelname)s - %(message)s')
loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
for logger in loggers:
    logger.setLevel(logging.DEBUG)


class TestNewAnalyse:

    def setup_class(self):
        client_name = 'kpn'

        self.kpn = KPNTestETL(client=client_name, config=config.client_config[client_name])
        self.kpn.perform()
        self.client_config = config.client_config[client_name]

        try:
            self.df_l_kpn = pickle.load(open(f"df_l_{client_name}.pickle", "rb"))
        except (OSError, IOError):
            etl = ExtractTransformProjectDataFirestoreToDfList(
                bucket=self.client_config["bucket"],
                projects=self.client_config["projects"],
                col=self.client_config["columns"]
            )
            self.df_l_kpn = etl.data
            pickle.dump(self.df_l_kpn, open(f"df_l_{client_name}.pickle", "wb"))

        try:
            self.analyse_old = pickle.load(open(f"analyse_old_{client_name}.pickle", "rb"))
        except (OSError, IOError):
            self.analyse_old = analyseKPN(client_name=client_name, df_l=self.df_l_kpn)
            pickle.dump(self.analyse_old, open(f"analyse_old_{client_name}.pickle", "wb"))

    def test_kpn_documents(self):

        # These fields are extracted using the following regex expression on Analysis.py  r"record_dict\.add\('(.*?)'"
        # Removed the fields for tmobile manually
        fields_from_Analysis_py = ['x_d', 'rc1', 'rc2', 'd_real_l_r', 'd_real_l_ri', 'y_prog_l', 'x_prog', 't_shift',
                                   'cutoff',
                                   'y_target_l', 'HC_HPend', 'HC_HPend_l', 'Schouw_BIS',
                                   'HPend_l', 'HAS_werkvoorraad',
                                   'graph_targets_W', 'graph_targets_M', 'jaaroverzicht', 'project_performance',
                                   'y_voorraad_act', 'project_names',
                                   'prognose_graph_dict', 'info_table', 'reden_na_overview', 'reden_na_projects',
                                   'reden_na_overview', 'reden_na_projects', 'analysis', 'analysis2', 'analysis3']

        assert self.kpn.client == "kpn"
        assert set(self.kpn.document_names()) == set(f"{self.kpn.client}_{x}" for x in fields_from_Analysis_py)

    def test_compare_old_new(self):
        assert set(self.kpn.record_dict) == set(self.analyse_old.record_dict)
        for document in self.kpn.record_dict:
            assert self.kpn.record_dict[document] == self.analyse_old.record_dict[document]
