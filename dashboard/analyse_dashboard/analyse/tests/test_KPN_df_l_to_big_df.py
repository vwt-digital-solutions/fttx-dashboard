from ETL import ExtractTransformProjectDataFirestoreToDfList, ExtractTransformProjectDataFirestore
from functions import get_start_time
from tests.old_functions import get_start_time_old
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
        concat_df = pd.concat(self.df_l.values())
        assert len(concat_df) == len(self.df)
        assert set(concat_df.sleutel) == set(self.df.sleutel)
        assert set(concat_df.project) == set(self.df.project)
        assert set(self.df_l.keys()) == set(self.client_config['projects'])
        assert set(self.df.project.cat.categories) == set(self.client_config['projects'])
        assert set(self.df_l.keys()) == set(self.df.project.cat.categories)

    def test_get_start_time(self):
        old_result = get_start_time_old(self.df_l)
        new_result = get_start_time(self.df)

        assert set(old_result.keys()) == set(new_result.keys())
        assert old_result == new_result
