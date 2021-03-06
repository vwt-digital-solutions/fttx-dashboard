import logging
import os

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
from Analyse.Indicators.ClientTargetIndicator import ClientTargetIndicator
from Analyse.Indicators.HASIngeplandIndicator import HASIngeplandIndicator
from Analyse.Indicators.HCOpen import HCOpen
from Analyse.Indicators.HCPatchOnly import HCPatchOnly
from Analyse.Indicators.InternalTargetHPcivielIndicator import \
    InternalTargetHPcivielIndicator
from Analyse.Indicators.InternalTargetHPendIndicator import \
    InternalTargetHPendIndicator
from Analyse.Indicators.InternalTargetHPendIntegratedIndicator import \
    InternalTargetHPendIntegratedIndicator
from Analyse.Indicators.InternalTargetTmobileIndicator import \
    InternalTargetTmobileIndicator
from Analyse.Indicators.LeverbetrouwbaarheidsIndictor import LeverbetrouwbaarheidIndicator
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
from Analyse.Indicators.RealisationHPcivielIndicator import \
    RealisationHPcivielIndicator
from Analyse.Indicators.RealisationHPendIndicator import \
    RealisationHPendIndicator
from Analyse.Indicators.RealisationHPendIntegratedIndicator import \
    RealisationHPendIntegratedIndicator
from Analyse.Indicators.RealisationHPendTmobileIndicator import \
    RealisationHPendTmobileIndicator
from Analyse.Indicators.RealisationHPendTmobileOnTimeIndicator import \
    RealisationHPendTmobileOnTimeIndicator
from Analyse.Indicators.RedenNaIndicator import RedenNaIndicator
from Analyse.Indicators.WerkvoorraadIndicator import WerkvoorraadIndicator
from Analyse.KPNDFN import KPNDFNExtract, KPNDFNTransform
from Analyse.Record.RecordList import RecordList
from Analyse.TMobile import TMobileTransform

logger = logging.getLogger("FttX Indicator Analyse")


class FttXIndicatorTransform(FttXTransform):
    def transform(self):
        super().transform()

    def _add_status_columns(self):
        return None

    def _set_totals(self):
        return None


class FttXIndicatorAnalyse(FttXBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.records = RecordList()

    # TODO: Documentation by Andre van Turnhout
    def analyse(self):
        df = self.transformed_data.df
        project_info = self.transformed_data.project_info
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
        self.records.append(HASIngeplandIndicator(df=df, client=self.client).perform())
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
                graph_name='leverbetrouwbaarheid'
            ).perform()
        )


class KPNDFNIndicatorAnalyse(FttXIndicatorAnalyse):
    def analyse(self):
        super().analyse()
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
            InternalTargetHPendIndicator(
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
    def analyse(self):
        super().analyse()
        df = self.transformed_data.df
        project_info = self.transformed_data.project_info
        self.records.append(
            InternalTargetTmobileIndicator(
                df=df, project_info=project_info, client=self.client
            ).perform()
        )
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
            RealisationHPendTmobileOnTimeIndicator(
                df=df, project_info=project_info, client=self.client
            ).perform()
        )


class KPNIndicatorAnalyse(KPNDFNIndicatorAnalyse):
    def analyse(self):
        super().analyse()
        planning_data = self.transformed_data.planning_new
        self.records.append(
            PlanningHPCivielIndicatorKPN(df=planning_data, client=self.client).perform()
        )
        self.records.append(
            PlanningHPEndIndicatorKPN(df=planning_data, client=self.client).perform()
        )
        self.records.append(
            ClientTargetIndicator(df=None, client=self.client).perform()
        )


class DFNIndicatorAnalyse(KPNDFNIndicatorAnalyse):
    def analyse(self):
        super().analyse()
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
    ...


class DFNIndicatorETL(
    FttXIndicatorETL, KPNDFNExtract, KPNDFNTransform, DFNIndicatorAnalyse
):
    ...


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
