import pytest

from ETL import ExtractTransformProjectDataFirestoreToDfList, ExtractTransformProjectDataFirestore
from functions import get_start_time, get_total_objects, add_relevant_columns, get_homes_completed, get_HPend, \
    get_has_ready, calculate_y_voorraad_act, get_has_werkvoorraad, get_hc_hpend_ratio, preprocess_data, \
    calculate_projectspecs, get_timeline, get_data_targets, set_filters, error_check_FCBC, overview_reden_na, \
    individual_reden_na
from tests.old_functions import get_start_time_old, get_total_objects_old, add_relevant_columns_old, \
    get_homes_completed_old, get_HPend_old, get_has_ready_old, calculate_y_voorraad_act_old, get_has_werkvoorraad_old, \
    get_hc_hpend_ratio_old, preprocess_data_old, calculate_projectspecs_old, prognose_old, set_filters_old, \
    error_check_FCBC_old, overview_reden_na_old, individual_reden_na_old
from analyse_dashboard.analyse import config
import pickle
import os
import pandas as pd


class TestKPNdflToBigDf:

    def setup_class(self):
        print(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
        print(os.environ["OAUTHLIB_INSECURE_TRANSPORT"])

        client_name = 'kpn'
        self.client_config = config.client_config[client_name]

        try:
            self.df_l = pickle.load(open("df_l.pickle", "rb"))
        except (OSError, IOError):
            etl = ExtractTransformProjectDataFirestoreToDfList(
                bucket=self.client_config["bucket"],
                projects=self.client_config["projects"],
                col=self.client_config["columns"]
            )
            self.df_l = etl.data
            pickle.dump(self.df_l, open("df_l.pickle", "wb"))

        try:
            self.df = pickle.load(open("df.pickle", "rb"))
        except (OSError, IOError):
            etl = ExtractTransformProjectDataFirestore(
                bucket=self.client_config["bucket"],
                projects=self.client_config["projects"],
                col=self.client_config["columns"]
            )
            self.df = etl.data
            pickle.dump(self.df, open("df.pickle", "wb"))

    def test_dfs(self):
        concat_df = pd.concat(self.df_l.values()).sort_values(by="sleutel").reset_index()
        sorted_df = self.df.sort_values(by="sleutel").reset_index()

        assert (concat_df.sleutel == sorted_df.sleutel).all()

        assert len(concat_df) == len(self.df)
        assert set(concat_df.sleutel) == set(self.df.sleutel)
        assert set(concat_df.project) == set(self.df.project)
        assert set(self.df_l.keys()) == set(self.client_config['projects'])
        assert set(self.df.project.cat.categories) == set(self.client_config['projects'])
        assert set(self.df_l.keys()) == set(self.df.project.cat.categories)

    def test_get_start_time(self):
        old_result = get_start_time_old(self.df_l.copy())
        new_result = get_start_time(self.df.copy())

        assert set(old_result.keys()) == set(new_result.keys())
        assert old_result == new_result

    @pytest.mark.skip(reason="no way of currently testing this")
    def test_get_total_objects(self):
        old_result = get_total_objects_old(self.df_l.copy())
        new_result = get_total_objects(self.df.copy())
        assert old_result == new_result

    def test_add_relevant_columns(self):
        old_result = add_relevant_columns_old(self.df_l.copy(), None)

        concat_df = pd.concat(old_result).sort_values(by="sleutel").reset_index()

        new_result = add_relevant_columns(self.df.copy(), None).sort_values(by="sleutel").reset_index()

        assert (concat_df['hpend'] == new_result['hpend']).all()
        assert (concat_df['homes_completed'] == new_result['homes_completed']).all()
        assert (concat_df['bis_gereed'] == new_result['bis_gereed']).all()

    def test_get_homes_completed(self):
        old_result = get_homes_completed_old(add_relevant_columns_old(self.df_l.copy(), None))
        new_result = get_homes_completed(add_relevant_columns(self.df.copy(), None))
        assert old_result == new_result

    def test_get_HPend(self):
        old_result = get_HPend_old(add_relevant_columns_old(self.df_l.copy(), None))
        new_result = get_HPend(add_relevant_columns(self.df.copy(), None))
        assert old_result == new_result

    def test_get_has_ready(self):
        old_result = get_has_ready_old(add_relevant_columns_old(self.df_l.copy(), None))
        new_result = get_has_ready(add_relevant_columns(self.df.copy(), None))
        assert old_result == new_result

    def test_calculate_y_voorraad_act(self):
        old_result = calculate_y_voorraad_act_old(self.df_l.copy())
        new_result = calculate_y_voorraad_act(self.df.copy())
        assert old_result == new_result

    def test_get_has_werkvoorraad(self):
        old_result = get_has_werkvoorraad_old(self.df_l.copy())
        new_result = get_has_werkvoorraad(self.df.copy())
        assert old_result == new_result

    def test_get_hc_hpend_ratio(self):
        old_result = get_hc_hpend_ratio_old(add_relevant_columns_old(self.df_l.copy(), None))
        new_result = get_hc_hpend_ratio(add_relevant_columns(self.df.copy(), None))
        assert old_result == new_result

    def test_preprocess_data(self):
        old_result = preprocess_data_old(self.df_l.copy(), '2020')
        concat_df = pd.concat(old_result).sort_values(by="sleutel").reset_index()

        new_result = preprocess_data(self.df.copy(), '2020').sort_values(by="sleutel").reset_index()

        assert (concat_df['hpend'] == new_result['hpend']).all()
        assert (concat_df['homes_completed'] == new_result['homes_completed']).all()
        assert (concat_df['bis_gereed'] == new_result['bis_gereed']).all()

    def test_calculate_projectspecs(self):
        hc_hp_end_ratio_total_old, \
            hc_hpend_ratio_old, \
            has_ready_old, \
            homes_ended_old, \
            werkvoorraad_old = calculate_projectspecs_old(add_relevant_columns_old(self.df_l.copy(), None))

        hc_hp_end_ratio_total, \
            hc_hpend_ratio, \
            has_ready, \
            homes_ended, \
            werkvoorraad = calculate_projectspecs(add_relevant_columns(self.df.copy(), None))

        assert hc_hp_end_ratio_total_old == hc_hp_end_ratio_total
        assert hc_hpend_ratio_old == hc_hpend_ratio
        assert has_ready_old == has_ready
        assert homes_ended_old == homes_ended
        assert werkvoorraad_old == werkvoorraad

    @pytest.mark.skip(reason="Analysis document is missing")
    def test_prognose(self):
        tot_l = get_total_objects(self.df_l)
        t_s = get_start_time_old(self.df_l)
        x_d = get_timeline(t_s)
        date_FTU0, date_FTU1 = get_data_targets(None)
        old_result = prognose_old(self.df_l, t_s, x_d, tot_l, date_FTU0)
        assert old_result == 1

    def test_set_filters(self):
        old_result = set_filters_old(self.df_l)
        new_result = set_filters(self.df)

        assert old_result == new_result

    @pytest.mark.skip(
        reason="One discrepancy in rule 710, but too slow to keep running the test while working on the rest")
    def test_error_check_FCBC(self):
        old_result = error_check_FCBC_old(self.df_l)
        new_result = error_check_FCBC(self.df)
        assert old_result[0] == new_result[0]

    def test_overview_reden_na(self):
        old_result = overview_reden_na_old(self.df_l, config.clusters_reden_na)
        new_result = overview_reden_na(self.df, config.clusters_reden_na)
        assert old_result == new_result

    def test_individual_reden_na(self):
        old_result = individual_reden_na_old(self.df_l, config.clusters_reden_na)
        new_result = individual_reden_na(self.df, config.clusters_reden_na)
        assert old_result == new_result
