import pickle

from Analyse.TMobile import TMobileTestETL
from analyse_dashboard.analyse import config
from Analyse.KPN import KPNTestETL

import logging

from tests.analyse_old.ETL import ExtractTransformProjectDataFirestoreToDfList, ExtractTransformProjectDataFirestore
from tests.analyse_old.main_to_test import analyseKPN, analyseTmobile

logging.basicConfig(format=' %(asctime)s - %(levelname)s - %(message)s')
loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
for logger in loggers:
    logger.setLevel(logging.DEBUG)


class TestNewAnalyse:

    def setup_class(self):
        client_name = 'kpn'
        client_config = config.client_config[client_name]

        self.kpn = KPNTestETL(client=client_name, config=client_config)
        self.kpn.perform()

        try:
            self.df_l_kpn = pickle.load(open(f"df_l_{client_name}.pickle", "rb"))
        except (OSError, IOError):
            etl = ExtractTransformProjectDataFirestoreToDfList(
                bucket=client_config["bucket"],
                projects=client_config["projects"],
                col=client_config["columns"]
            )
            self.df_l_kpn = etl.data
            pickle.dump(self.df_l_kpn, open(f"df_l_{client_name}.pickle", "wb"))

        try:
            self.analyse_old_kpn = pickle.load(open(f"analyse_old_{client_name}.pickle", "rb"))
        except (OSError, IOError):
            self.analyse_old_kpn = analyseKPN(client_name=client_name, df_l=self.df_l_kpn)
            pickle.dump(self.analyse_old_kpn, open(f"analyse_old_{client_name}.pickle", "wb"))

        client_name = 't-mobile'
        client_config = config.client_config[client_name]

        self.tmobile = TMobileTestETL(client=client_name, config=client_config)
        self.tmobile.perform()

        try:
            self.df_tmobile = pickle.load(open(f"df_{client_name}.pickle", "rb"))
        except (OSError, IOError):
            etl = ExtractTransformProjectDataFirestore(
                client_config["bucket"],
                client_config["projects"],
                client_config["columns"],
            )
            self.df_tmobile = etl.data
            pickle.dump(self.df_tmobile, open(f"df_{client_name}.pickle", "wb"))
        self.analyse_old_tmobile = analyseTmobile(client_name=client_name, df_l=self.df_tmobile)

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

    def test_compare_old_new_kpn(self):
        assert set(self.kpn.record_dict) == set(self.analyse_old_kpn.record_dict)
        for document in self.kpn.record_dict:
            assert self.kpn.record_dict[document] == self.analyse_old_kpn.record_dict[document]

    def test_compare_old_new_tmobile(self):
        assert set(self.analyse_old_tmobile.record_dict) <= set(self.tmobile.record_dict)
        for document in self.analyse_old_tmobile.record_dict:
            assert self.analyse_old_tmobile.record_dict[document] == self.tmobile.record_dict[document]
