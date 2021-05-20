import logging
import os

import numpy as np
import pandas as pd

import business_rules as br
from Analyse.ETL import ETL
from Analyse.FttX import (FttXBase, FttXExtract, FttXLoad, FttXTestLoad,
                          FttXTransform, PickleExtract)
from Analyse.Indicators.ActualRedenNAHCopenLateIndicator import \
    ActualRedenNAHCopenLateIndicator
from Analyse.Indicators.ActualRedenNAHCopenOnTimeIndicator import \
    ActualRedenNAHCopenOnTimeIndicator
from Analyse.Indicators.ActualRedenNAHCopenTooLateIndicator import \
    ActualRedenNAHCopenTooLateIndicator
from Analyse.Indicators.ActualRedenNAPatchOnlyLateIndicator import \
    ActualRedenNAPatchOnlyLateIndicator
from Analyse.Indicators.ActualRedenNAPatchOnlyOnTimeIndicator import \
    ActualRedenNAPatchOnlyOnTimeIndicator
from Analyse.Indicators.ActualRedenNAPatchOnlyTooLateIndicator import \
    ActualRedenNAPatchOnlyTooLateIndicator
from Analyse.Indicators.ActualStatusBarChartIndicator import \
    ActualStatusBarChartIndicator
from Analyse.Indicators.ClientTargetKPNIndicator import \
    ClientTargetKPNIndicator
from Analyse.Indicators.HCOpen import HCOpen
from Analyse.Indicators.HCPatchOnly import HCPatchOnly
from Analyse.Indicators.InternalTargetHPcivielIndicator import \
    InternalTargetHPcivielIndicator
from Analyse.Indicators.InternalTargetHPendIndicator import \
    InternalTargetHPendIndicator
from Analyse.Indicators.InternalTargetHPendIntegratedIndicator import \
    InternalTargetHPendIntegratedIndicator
from Analyse.Indicators.LeverbetrouwbaarheidsIndicator import \
    LeverbetrouwbaarheidIndicator
from Analyse.Indicators.PerformanceGraphIndicator import \
    PerformanceGraphIndicator
from Analyse.Indicators.PlanningHPCivielIndicatorKPN import \
    PlanningHPCivielIndicatorKPN
from Analyse.Indicators.PlanningHPEndIndicatorKPN import \
    PlanningHPEndIndicatorKPN
from Analyse.Indicators.PlanningIndicatorDFN import PlanningIndicatorDFN
from Analyse.Indicators.PlanningIndicatorTMobile import \
    PlanningIndicatorTMobile
from Analyse.Indicators.PrognoseIndicator import PrognoseIndicator
from Analyse.Indicators.PrognoseIntegratedIndicator import \
    PrognoseIntegratedIndicator
from Analyse.Indicators.RealisationHCIndicator import RealisationHCIndicator
from Analyse.Indicators.RealisationHCIntegratedIndicator import \
    RealisationHCIntegratedIndicator
from Analyse.Indicators.RealisationHCIntegratedTmobileIndicator import \
    RealisationHCIntegratedTmobileIndicator
from Analyse.Indicators.RealisationHCIntegratedTmobileOnTimeIndicator import \
    RealisationHCIntegratedTmobileOnTimeIndicator
from Analyse.Indicators.RealisationHCTmobileIndicator import \
    RealisationHCTmobileIndicator
from Analyse.Indicators.RealisationHCTmobileOnTimeIndicator import \
    RealisationHCTmobileOnTimeIndicator
from Analyse.Indicators.RealisationHPcivielIndicator import \
    RealisationHPcivielIndicator
from Analyse.Indicators.RealisationHPendIndicator import \
    RealisationHPendIndicator
from Analyse.Indicators.RealisationHPendIntegratedIndicator import \
    RealisationHPendIntegratedIndicator
from Analyse.Indicators.RealisationHPendTmobileIndicator import \
    RealisationHPendTmobileIndicator
from Analyse.Indicators.RedenNaIndicator import RedenNaIndicator
from Analyse.Indicators.WerkvoorraadIndicator import WerkvoorraadIndicator
from Analyse.KPNDFN import KPNDFNExtract, KPNDFNTransform
from Analyse.Record.DocumentListRecord import DocumentListRecord
from Analyse.Record.ListRecord import ListRecord
from Analyse.Record.Record import Record
from Analyse.Record.RecordList import RecordList
from Analyse.TMobile import TMobileTransform
from functions import create_project_filter

logger = logging.getLogger("FttX Indicator Analyse")


class FttXIndicatorTransform(FttXTransform):
    def transform(self):
        super().transform()


class FttXIndicatorAnalyse(FttXBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.records = RecordList()

    # TODO: Documentation by Andre van Turnhout
    def analyse(self):
        df = self.transformed_data.df
        project_info = self.transformed_data.project_info

        self.records.append(self._set_filters(client=self.client))

        self.records.append(self._calculate_list_of_years(client=self.client))

        self.records.append(self._progress_per_phase_over_time_for_finance())

        self.records.append(self._progress_per_phase_for_finance())

        self.records.append(RedenNaIndicator(df=df, client=self.client).perform())

        self.records.append(
            ActualStatusBarChartIndicator(df=df, client=self.client).perform()
        )

        self.records.append(WerkvoorraadIndicator(df=df, client=self.client).perform())

        self.records.append(
            RealisationHPcivielIndicator(
                df=df, project_info=project_info, client=self.client
            ).perform()
        )

        self.records.append(
            PerformanceGraphIndicator(
                df=df,
                project_info=self.transformed_data.project_info,
                client=self.client,
            ).perform()
        )

        self.records.append(
            LeverbetrouwbaarheidIndicator(
                df=df,
                client=self.client,
            ).perform()
        )

        self.records.append(
            InternalTargetHPendIndicator(
                project_info=project_info, client=self.client
            ).perform()
        )

    def _set_filters(self, client):
        """
        Sets the set of projects that should be shown in the dashboard as record, so that it can be retrieved from the
        firestore.

        """
        return ListRecord(
            record=create_project_filter(self.transformed_data.df),
            graph_name="project_names",
            collection="Data",
            client=client,
        )

    def _progress_per_phase_over_time_for_finance(self):
        """
        This function calculates the progress per phase over time base on the specified columns
        per phase:
            'opleverdatum': 'has',
            'schouwdatum': 'schouwen',
            'laswerkapgereed_datum': 'montage ap',
            'laswerkdpgereed_datum': 'montage dp',
            'status_civiel_datum': 'civiel'

        Adds a record consisting of dict per project holding a timeindex with progress per phase

        """
        logger.info("Calculating project progress per phase over time")
        document_list = []
        for project, df in self.transformed_data.df.groupby("project"):
            if df.empty:
                continue
            columns = [
                "opleverdatum",
                "schouwdatum",
                "laswerkapgereed_datum",
                "laswerkdpgereed_datum",
                "status_civiel_datum",
                "laswerkapgereed",
                "laswerkdpgereed",
            ]
            date_df = df.loc[:, columns]
            mask = br.laswerk_dp_gereed(df) & br.laswerk_ap_gereed(df)
            date_df["montage"] = np.datetime64("NaT")
            date_df.loc[mask, "montage"] = date_df[
                ["laswerkapgereed_datum", "laswerkdpgereed_datum"]
            ][mask].max(axis=1)
            date_df = date_df.drop(columns=["laswerkapgereed", "laswerkdpgereed"])
            progress_over_time: pd.DataFrame = date_df.apply(pd.value_counts).resample(
                "D"
            ).sum().cumsum() / len(df)
            progress_over_time.index = progress_over_time.index.strftime("%Y-%m-%d")
            progress_over_time.rename(
                columns={
                    "opleverdatum": "has",
                    "schouwdatum": "schouwen",
                    "laswerkapgereed_datum": "montage ap",
                    "laswerkdpgereed_datum": "montage dp",
                    "status_civiel_datum": "civiel",
                },
                inplace=True,
            )
            record = progress_over_time.to_dict()
            document_list.append(
                dict(
                    client=self.client,
                    project=project,
                    data_set="progress_over_time",
                    record=record,
                )
            )

        return DocumentListRecord(
            record=document_list,
            client=self.client,
            collection="Data",
            graph_name="Progress_over_time",
            document_key=["client", "project", "data_set"],
        )

    def _progress_per_phase_for_finance(self):
        """
        Calculates the progress per phase for the phases civiel, montage, schouwen, hc, hp and hp end, as well
        as the totals per project. These results are put in a record and added to the records attribute of the class.

        """
        logger.info("Calculating project progress per phase")

        progress_df = pd.concat(
            [
                self.transformed_data.df.project,
                ~self.transformed_data.df.sleutel.isna(),
                self.transformed_data.df.status_civiel.str.contains("1"),
                br.laswerk_dp_gereed(self.transformed_data.df)
                & br.laswerk_ap_gereed(self.transformed_data.df),
                br.geschouwed(self.transformed_data.df),
                br.hc_opgeleverd(self.transformed_data.df),
                br.hp_opgeleverd(self.transformed_data.df),
                br.opgeleverd(self.transformed_data.df),
            ],
            axis=1,
        )
        progress_df.columns = [
            "project",
            "totaal",
            "civiel",
            "montage",
            "schouwen",
            "hc",
            "hp",
            "hpend",
        ]
        documents = [
            dict(
                project=project, client=self.client, data_set="progress", record=values
            )
            for project, values in progress_df.groupby("project")
            .sum()
            .to_dict(orient="index")
            .items()
        ]

        return DocumentListRecord(
            record=documents,
            client=self.client,
            collection="Data",
            graph_name="Progress",
            document_key=["client", "project", "data_set"],
        )

    def _calculate_list_of_years(self, client):
        """
        Calculates a list of years per client based on the dates that are found in the date columns.

        """
        logger.info("Calculating list of years")
        date_columns = self.transformed_data.datums
        dc_data = self.transformed_data.df.loc[:, date_columns]
        list_of_years = []
        for col in dc_data.columns:
            list_of_years += list(dc_data[col].dropna().dt.year.unique().astype(str))
        list_of_years = sorted(list(set(list_of_years)))

        return Record(
            record=list_of_years,
            collection="Data",
            graph_name="List_of_years",
            client=client,
        )


class KPNDFNIndicatorAnalyse(FttXIndicatorAnalyse):
    """Main class to for running the indicator analysis for KPN and DFN"""

    def analyse_1(self):
        """First part of the analyse that runs the general indicators inheritted from the FttXIndicatorAnalyse"""
        super().analyse()

    def analyse_2(self):
        """Second part of the analyse that runs the speficiec indicators for KPN and DFN"""
        df = self.transformed_data.df
        project_info = self.transformed_data.project_info

        self.records.append(
            PrognoseIntegratedIndicator(
                df=df, client=self.client, project_info=project_info
            ).perform()
        )
        self.records.append(
            PrognoseIndicator(
                df=df, client=self.client, project_info=project_info
            ).perform()
        )
        self.records.append(
            InternalTargetHPcivielIndicator(
                project_info=project_info, client=self.client
            ).perform()
        )
        self.records.append(
            InternalTargetHPendIntegratedIndicator(
                project_info=project_info, client=self.client
            ).perform()
        )
        self.records.append(
            RealisationHPendIndicator(
                df=df, project_info=project_info, client=self.client
            ).perform()
        )
        self.records.append(
            RealisationHCIndicator(
                df=df, project_info=project_info, client=self.client
            ).perform()
        )
        self.records.append(
            RealisationHPendIntegratedIndicator(
                df=df, project_info=project_info, client=self.client
            ).perform()
        )
        self.records.append(
            RealisationHCIntegratedIndicator(
                df=df, project_info=project_info, client=self.client
            ).perform()
        )


class TmobileIndicatorAnalyse(FttXIndicatorAnalyse):
    """Main class to run indicator analyse for T-mobile"""

    def analyse(self):
        super().analyse()
        df = self.transformed_data.df
        project_info = self.transformed_data.project_info

        self.records.append(HCPatchOnly(df=df, client=self.client).perform())
        self.records.append(HCOpen(df=df, client=self.client).perform())
        self.records.append(
            ActualRedenNAHCopenOnTimeIndicator(df=df, client=self.client).perform()
        )
        self.records.append(
            ActualRedenNAHCopenLateIndicator(df=df, client=self.client).perform()
        )
        self.records.append(
            ActualRedenNAHCopenTooLateIndicator(df=df, client=self.client).perform()
        )
        self.records.append(
            ActualRedenNAPatchOnlyOnTimeIndicator(df=df, client=self.client).perform()
        )
        self.records.append(
            ActualRedenNAPatchOnlyLateIndicator(df=df, client=self.client).perform()
        )
        self.records.append(
            ActualRedenNAPatchOnlyTooLateIndicator(df=df, client=self.client).perform()
        )
        self.records.append(
            PlanningIndicatorTMobile(df=df, client=self.client).perform()
        )
        self.records.append(
            RealisationHPendTmobileIndicator(
                df=df, project_info=project_info, client=self.client
            ).perform()
        )
        self.records.append(
            RealisationHCTmobileIndicator(
                df=df, project_info=project_info, client=self.client
            ).perform()
        )
        self.records.append(
            RealisationHCTmobileOnTimeIndicator(
                df=df, project_info=project_info, client=self.client
            ).perform()
        )
        self.records.append(
            RealisationHCIntegratedTmobileOnTimeIndicator(
                df=df, project_info=project_info, client=self.client
            ).perform()
        )
        self.records.append(
            RealisationHCIntegratedTmobileIndicator(
                df=df, project_info=project_info, client=self.client
            ).perform()
        )


class KPNIndicatorAnalyse(KPNDFNIndicatorAnalyse):
    def analyse_1(self):
        super().analyse_1()

    def analyse_2(self):
        super().analyse_2()
        planning_data = self.transformed_data.planning_new
        self.records.append(
            PlanningHPCivielIndicatorKPN(df=planning_data, client=self.client).perform()
        )
        self.records.append(
            PlanningHPEndIndicatorKPN(df=planning_data, client=self.client).perform()
        )
        self.records.append(
            ClientTargetKPNIndicator(df=None, client=self.client).perform()
        )


class DFNIndicatorAnalyse(KPNDFNIndicatorAnalyse):
    def analyse(self):
        super().analyse_1()
        super().analyse_2()
        df = self.transformed_data.df
        self.records.append(PlanningIndicatorDFN(df=df, client=self.client).perform())


class FttXIndicatorETL(
    ETL, FttXExtract, FttXIndicatorAnalyse, FttXIndicatorTransform, FttXLoad
):
    def perform(self):
        self.extract()
        self.transform()
        self.analyse()
        self.load()


class KPNIndicatorETL(
    FttXIndicatorETL, KPNDFNExtract, KPNDFNTransform, KPNIndicatorAnalyse
):
    def perform(self):
        super().extract()
        super().transform()
        super().analyse_1()
        super().analyse_2()
        super().load()

    def perform_1(self):
        super().extract()
        super().transform()
        super().analyse_1()
        super().load()

    def perform_2(self):
        super().extract()
        super().transform()
        super().analyse_2()
        super().load()


class DFNIndicatorETL(
    FttXIndicatorETL, KPNDFNExtract, KPNDFNTransform, DFNIndicatorAnalyse
):
    def perform(self):
        super().extract()
        super().transform()
        super().analyse_1()
        super().analyse_2()
        super().load()


class TmobileIndicatorETL(FttXIndicatorETL, TMobileTransform, TmobileIndicatorAnalyse):
    ...


class FttXIndicatorTestETL(PickleExtract, FttXIndicatorETL):
    ...


class KPNIndicatorTestETL(PickleExtract, KPNIndicatorETL, FttXTestLoad):
    ...


class DFNIndicatorTestETL(
    PickleExtract, DFNIndicatorETL, FttXTestLoad, DFNIndicatorAnalyse
):
    ...


class TmobileIndicatorTestETL(
    PickleExtract, TmobileIndicatorETL, FttXTestLoad, TmobileIndicatorAnalyse
):
    ...


class FttXIndicatorLocalETL(PickleExtract, FttXIndicatorETL):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def load(self):
        if "FIRESTORE_EMULATOR_HOST" in os.environ:
            logger.info("Loading into emulated firestore")
            super().load()
        else:
            logger.warning(
                "Attempting to load with a local ETL process but no emulator is configured. Loading aborted."
            )


class KPNIndicatorLocalETL(KPNIndicatorETL, FttXIndicatorLocalETL):
    ...


class DFNIndicatorLocalETL(DFNIndicatorETL, FttXIndicatorLocalETL):
    ...


class TmobileIndicatorLocalETL(FttXIndicatorLocalETL, TmobileIndicatorETL):
    ...
