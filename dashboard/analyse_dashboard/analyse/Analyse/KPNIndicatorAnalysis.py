from Analyse.ETL import ETL
from Analyse.FttX import FttXBase, FttXTestLoad, FttXExtract, FttXTransform, FttXLoad
from Analyse.Indicators.InternalTargetIndicator import InternalTargetIndicator
from Analyse.Indicators.InternalTargetTmobileIndicator import InternalTargetTmobileIndicator
from Analyse.Indicators.PrognoseIndicator import PrognoseIndicator
from Analyse.Indicators.RealisationHPcivielIndicator import RealisationHPcivielIndicator
from Analyse.Indicators.RealisationHPendIndicator import RealisationHPendIndicator
from Analyse.Indicators.RedenNaOverviewIndicator import RedenNaOverviewIndicator
from Analyse.Indicators.RedenNaProjectIndicator import RedenNaProjectIndicator
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
        self.records.append(PrognoseIndicator(df=df,
                                              client=self.client,
                                              project_info=project_info).perform())
        self.records.append(RealisationHPcivielIndicator(df=df, client=self.client).perform())
        self.records.append(RealisationHPendIndicator(df=df, client=self.client).perform())
        self.records.append(RedenNaOverviewIndicator(df=df, client=self.client).perform())
        self.records.append(RedenNaProjectIndicator(df=df, client=self.client).perform())


class KPNDFNIndicatorAnalyse(FttXIndicatorAnalyse):

    def analyse(self):
        df = self.transformed_data.df
        self.records.append(WerkvoorraadIndicator(df=df, client=self.client))
        self.records.append(InternalTargetIndicator(df=df, client=self.client))


class TmobileIndicatorAnalyse(FttXIndicatorAnalyse):

    def analyse(self):
        df = self.transformed_data.df
        self.records.append(InternalTargetTmobileIndicator(df=df, client=self.client))


class FttXIndicatorETL(ETL, FttXExtract, FttXIndicatorAnalyse, FttXTransform, FttXLoad):
    ...


class KPNDFNIndicatorETL(FttXIndicatorETL, KPNDFNExtract, KPNDFNTransform, KPNDFNIndicatorAnalyse):
    ...


class TmobileIndicatorETL(FttXIndicatorAnalyse, TMobileTransform, TmobileIndicatorAnalyse):
    ...


class KPNIndicatorTestETL(FttXTestLoad, KPNDFNIndicatorAnalyse):
    ...
