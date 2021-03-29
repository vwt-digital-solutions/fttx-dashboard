from Analyse.ETL import ETL
from Analyse.FttX import (FttXBase, FttXExtract, FttXLoad, FttXTestLoad,
                          FttXTransform)
from Analyse.Indicators.HASIngeplandIndicator import HASIngeplandIndicator
from Analyse.Indicators.HcHpEndIndicator import HcHpEndIndicator
from Analyse.Indicators.HcPatch import HcPatch
from Analyse.Indicators.InternalTargetHPcivielIndicator import \
    InternalTargetHPcivielIndicator
from Analyse.Indicators.InternalTargetHPendIndicator import \
    InternalTargetHPendIndicator
from Analyse.Indicators.InternalTargetTmobileIndicator import \
    InternalTargetTmobileIndicator
from Analyse.Indicators.PlanningHPCivielIndicatorKPN import \
    PlanningHPCivielIndicatorKPN
from Analyse.Indicators.PlanningHPEndIndicatorKPN import \
    PlanningHPEndIndicatorKPN
from Analyse.Indicators.PlanningIndicatorTMobile import \
    PlanningIndicatorTMobile
from Analyse.Indicators.PrognoseIndicator import PrognoseIndicator
from Analyse.Indicators.RealisationHPcivielIndicator import \
    RealisationHPcivielIndicator
from Analyse.Indicators.RealisationHPendIndicator import \
    RealisationHPendIndicator
from Analyse.Indicators.RedenNaOverviewIndicator import \
    RedenNaOverviewIndicator
from Analyse.Indicators.RedenNaProjectIndicator import RedenNaProjectIndicator
from Analyse.Indicators.TwelveWeekRatioIndicator import \
    TwelveWeekRatioIndicator
from Analyse.Indicators.WerkvoorraadIndicator import WerkvoorraadIndicator
from Analyse.KPNDFN import KPNDFNExtract, KPNDFNTransform
from Analyse.Record.RecordList import RecordList
from Analyse.TMobile import TMobileTransform


class FttXIndicatorAnalyse(FttXBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.records = RecordList()

    # TODO: Documentation by Andre van Turnhout
    def analyse(self):
        df = self.transformed_data.df
        project_info = self.transformed_data.project_info
        self.records.append(
            RedenNaOverviewIndicator(df=df, client=self.client).perform()
        )
        self.records.append(
            RedenNaProjectIndicator(df=df, client=self.client).perform()
        )

        self.records.append(WerkvoorraadIndicator(df=df, client=self.client).perform())

        self.records.append(
            RealisationHPcivielIndicator(
                df=df, project_info=project_info, client=self.client
            ).perform()
        )
        self.records.append(
            RealisationHPendIndicator(
                df=df, project_info=project_info, client=self.client
            ).perform()
        )

        self.records.append(HASIngeplandIndicator(df=df, client=self.client).perform())


class KPNDFNIndicatorAnalyse(FttXIndicatorAnalyse):
    def analyse(self):
        super().analyse()
        df = self.transformed_data.df
        project_info = self.transformed_data.project_info
        planning_data = self.transformed_data.planning_new
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
        self.records.append(HcHpEndIndicator(df=df, client=self.client).perform())
        self.records.append(
            PlanningHPCivielIndicatorKPN(df=planning_data, client=self.client).perform()
        )
        self.records.append(
            PlanningHPEndIndicatorKPN(df=planning_data, client=self.client).perform()
        )


class TmobileIndicatorAnalyse(FttXIndicatorAnalyse):
    def analyse(self):
        df = self.transformed_data.df
        project_info = self.transformed_data.project_info
        self.records.append(
            InternalTargetTmobileIndicator(
                df=df, project_info=project_info, client=self.client
            ).perform()
        )
        self.records.append(HcPatch(df=df, client=self.client).perform())
        self.records.append(
            PlanningIndicatorTMobile(df=df, client=self.client).perform()
        )
        self.records.append(
            TwelveWeekRatioIndicator(df=df, client=self.client).perform()
        )


class FttXIndicatorETL(ETL, FttXExtract, FttXIndicatorAnalyse, FttXTransform, FttXLoad):
    ...


class KPNDFNIndicatorETL(
    FttXIndicatorETL, KPNDFNExtract, KPNDFNTransform, KPNDFNIndicatorAnalyse
):
    ...


class TmobileIndicatorETL(FttXIndicatorETL, TMobileTransform, TmobileIndicatorAnalyse):
    ...


class KPNDFNIndicatorTestETL(FttXIndicatorETL, FttXTestLoad, KPNDFNIndicatorAnalyse):
    ...
