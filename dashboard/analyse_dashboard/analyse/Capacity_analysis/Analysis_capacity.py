from Analyse.Capacity_analysis.PhaseCapacity import LasAPCapacity
from Analyse.Capacity_analysis.PhaseCapacity.GeulenCapacity import GeulenCapacity
from Analyse.Capacity_analysis.PhaseCapacity.LasDPCapacity import LasDPCapacity
from Analyse.Capacity_analysis.PhaseCapacity.OpleverCapacity import OpleverCapacity
from Analyse.ETL import Extract, ETL, Transform, Load
from Analyse.FttX import FttXExtract, FttXTransform, FttXLoad


class CapacityExtract(Extract):
    ...


class CapacityTransform(Transform):
    ...


class CapacityLoad(Load):
    ...


class CapacityAnalyse():
    def __init__(self, phases):
        self.phases = phases

    def analyse(self):
        self.get_lines_per_phase()

    def get_lines_per_phase(self):
        for project, project_df in self.transformed_data.df.projects:
            GeulenCapacity(project_df[self.phases['geulen']['phase_column']]).algorithm().get_record()
            LasAPCapacity(project_df[self.phases['lasap']['phase_column']]).algorithm().get_record()
            LasDPCapacity(project_df[self.phases['lasdp']['phase_column']]).algorithm().get_record()
            OpleverCapacity(project_df[self.phases['oplever']['phase_column']]).algorithm().get_record()


class FactoryETL(ETL, FttXExtract, CapacityAnalyse, FttXTransform, FttXLoad):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def perform(self):
        self.extract()
        self.transform()
        self.analyse()
        self.load()
