import copy

import business_rules as br
from Analyse.Indicators.DataIndicator import DataIndicator
from Analyse.Record.DictRecord import DictRecord


class ActualStatusBarChartIndicator(DataIndicator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.collection = "Indicators"
        self.graph_name = "ActualStatusBarChartIndicator"

    def apply_business_rules(self):
        """
        Creates additional columns at the project status dataframe which
        contain information on the status of a house for a given phase.
        This information is required to make the aggregated dataframe for the
        actual status barchart at the frontend.

        Returns: a dataframe with columns on the status of a house, for given phases.
        """
        df = copy.deepcopy(self.df)

        df["HAS_status"] = False
        df.loc[br.has_niet_opgeleverd(df), "HAS_status"] = "niet_opgeleverd"
        df.loc[br.has_ingeplanned(df), "HAS_status"] = "ingeplanned"
        df.loc[br.hp_opgeleverd(df), "HAS_status"] = "opgeleverd_zonder_hc"
        df.loc[br.hc_opgeleverd(df), "HAS_status"] = "opgeleverd"

        df["schouw_status"] = False
        df.loc[~br.toestemming_bekend(df), "schouw_status"] = "niet_opgeleverd"
        df.loc[br.toestemming_bekend(df), "schouw_status"] = "opgeleverd"

        df["bis_status"] = False
        df.loc[br.bis_niet_opgeleverd(df), "bis_status"] = "niet_opgeleverd"
        df.loc[br.bis_opgeleverd(df), "bis_status"] = "opgeleverd"

        df["lasDP_status"] = False
        df.loc[br.laswerk_dp_niet_gereed(df), "lasDP_status"] = "niet_opgeleverd"
        df.loc[br.laswerk_dp_gereed(df), "lasDP_status"] = "opgeleverd"

        df["lasAP_status"] = False
        df.loc[br.laswerk_ap_niet_gereed(df), "lasAP_status"] = "niet_opgeleverd"
        df.loc[br.laswerk_ap_gereed(df), "lasAP_status"] = "opgeleverd"

        df["laagbouw"] = False
        df.loc[df["soort_bouw"] == "Laag", "laagbouw"] = True

        df_out = df[
            [
                "schouw_status",
                "bis_status",
                "laagbouw",
                "lasDP_status",
                "lasAP_status",
                "HAS_status",
                "cluster_redenna",
                "sleutel",
                "project",
            ]
        ]

        return df_out

    def perform(self):
        """
        Aggregates the dataframe with information on the status of houses for given phases.
        The aggregate contains the number of houses that belong to a specific selection of
        states at different phases.

        Returns: Record ready to be written to the firestore, containing clustered data per project.

        """
        df = self.apply_business_rules()
        df = df.rename(columns={"sleutel": "count"})
        col_names = list(df.columns)
        project_dict = {}
        for project in df.project.unique():
            project_status = df[df.project == project][col_names[:-1]]
            project_dict[project] = (
                project_status.groupby(col_names[:-2])
                .count()
                .reset_index()
                .dropna()
                .to_dict(orient="records")
            )

        return self.to_record(project_dict)

    def to_record(self, project_dict):
        dict_record = DictRecord(
            record=project_dict,
            collection="Indicators",
            client=self.client,
            graph_name=self.graph_name,
        )
        return dict_record
