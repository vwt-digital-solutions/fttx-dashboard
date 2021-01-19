from Analyse.Capacity_analysis.PhaseCapacity.GeulenCapacity import GeulenCapacity
from Analyse.Capacity_analysis.PhaseCapacity.LasAPCapacity import LasAPCapacity
from Analyse.Capacity_analysis.PhaseCapacity.LasDPCapacity import LasDPCapacity
from Analyse.Capacity_analysis.PhaseCapacity.OpleverCapacity import OpleverCapacity
from Analyse.Capacity_analysis.PhaseCapacity.SchietenCapacity import SchietenCapacity
from Analyse.ETL import Extract, Transform, Load
from Analyse.FttX import FttXTestLoad, PickleExtract, FttXExtract, FttXTransform
from datetime import timedelta
import pandas as pd

from Analyse.Record.RecordList import RecordList


class CapacityExtract(Extract):
    ...


class CapacityTransform(Transform):
    ...


class CapacityLoad(Load):
    ...


class CapacityAnalyse():
    def __init__(self):
        """
        Main class for the analyses required for the capacity planning algorithm. Per project and phase,
        required indicators are calculated and stored in dedicated phase objects. At the moment,
        the parameters performance_norm_config, n_days_config, phases_config, civil_date, total_units and
        phases_projectspecific are defined here but will be moved to config or transform in the following ticket.

        """
        # the parameters below need to come from config and transform
        self.performance_norm_config = 1  # % per day
        self.n_days_config = 100 / self.performance_norm_config
        self.phases_config = dict(geulen=dict(phase_column='opleverdatum', phase_delta=0, n_days=self.n_days_config),
                                  schieten=dict(phase_column='opleverdatum', phase_delta=0, n_days=self.n_days_config),
                                  lasap=dict(phase_column='opleverdatum', phase_delta=10, n_days=self.n_days_config),
                                  lasdp=dict(phase_column='opleverdatum', phase_delta=10, n_days=self.n_days_config),
                                  oplever=dict(phase_column='opleverdatum', phase_delta=20, n_days=self.n_days_config)
                                  )
        # project specific
        self.civil_date = dict(project1=pd.to_datetime('2021-01-01'))
        self.total_units = dict(project1=dict(geulen=1000, schieten=500, lasap=200, lasdp=200, oplever=5000))
        self.phases_projectspecific = dict(geulen=dict(start_date=self.civil_date['project1'] +
                                                       timedelta(days=self.phases_config['geulen']['phase_delta']),
                                                       total_units=self.total_units['project1']['geulen'],
                                                       performance_norm_unit=self.performance_norm_config / 100 *
                                                       self.total_units['project1']['geulen']),
                                           schieten=dict(start_date=self.civil_date['project1'] +
                                                         timedelta(days=self.phases_config['schieten']['phase_delta']),
                                                         total_units=self.total_units['project1']['schieten'],
                                                         performance_norm_unit=self.performance_norm_config / 100 *
                                                         self.total_units['project1']['schieten']),
                                           lasap=dict(start_date=self.civil_date['project1'] +
                                                      timedelta(days=self.phases_config['lasap']['phase_delta']),
                                                      total_units=self.total_units['project1']['lasap'],
                                                      performance_norm_unit=self.performance_norm_config / 100 *
                                                      self.total_units['project1']['lasap']),
                                           lasdp=dict(start_date=self.civil_date['project1'] +
                                                      timedelta(days=self.phases_config['lasdp']['phase_delta']),
                                                      total_units=self.total_units['project1']['lasdp'],
                                                      performance_norm_unit=self.performance_norm_config / 100 *
                                                      self.total_units['project1']['lasdp']),
                                           oplever=dict(start_date=self.civil_date['project1'] +
                                                        timedelta(days=self.phases_config['oplever']['phase_delta']),
                                                        total_units=self.total_units['project1']['oplever'],
                                                        performance_norm_unit=self.performance_norm_config / 100 *
                                                        self.total_units['project1']['oplever']))

    def analyse(self):
        self.get_lines_per_phase()

    def get_lines_per_phase(self):
        """
        Main loop to make capacity objects for all projects. Will fill record dict with LineRecord objects.
        """
        line_record_list = RecordList()
        for project, project_df in self.transformed_data.df.groupby(by="project"):
            line_record_list += GeulenCapacity(df=self.transformed_data.df[self.phases_config['geulen']['phase_column']],
                                               phases_config=self.phases_config['geulen'],
                                               phases_projectspecific=self.phases_projectspecific['geulen'],
                                               phase='geulen',
                                               client=self.client
                                               ).algorithm().get_record()
            line_record_list += SchietenCapacity(df=self.transformed_data.df[self.phases_config['schieten']['phase_column']],
                                                 phases_config=self.phases_config['schieten'],
                                                 phases_projectspecific=self.phases_projectspecific['schieten'],
                                                 phase='schieten',
                                                 client=self.client
                                                 ).algorithm().get_record()
            line_record_list += LasAPCapacity(df=self.transformed_data.df[self.phases_config['lasap']['phase_column']],
                                              phases_config=self.phases_config['lasap'],
                                              phases_projectspecific=self.phases_projectspecific['lasap'],
                                              phase='lasap',
                                              client=self.client
                                              ).algorithm().get_record()
            line_record_list += LasDPCapacity(df=self.transformed_data.df[self.phases_config['lasdp']['phase_column']],
                                              phases_config=self.phases_config['lasdp'],
                                              phases_projectspecific=self.phases_projectspecific['lasdp'],
                                              phase='lasdp',
                                              client=self.client
                                              ).algorithm().get_record()
            line_record_list += OpleverCapacity(df=self.transformed_data.df[self.phases_config['oplever']['phase_column']],
                                                phases_config=self.phases_config['oplever'],
                                                phases_projectspecific=self.phases_projectspecific['oplever'],
                                                phase='oplever',
                                                client=self.client
                                                ).algorithm().get_record()


class CapacityETL(FttXExtract, FttXTransform, CapacityAnalyse):
    """
    Main class to perform the ETL and analysis for capacity analysis for FttX. Will write records to the firestore.
    """
    ...


class CapacityTestETL(PickleExtract, FttXTestLoad, CapacityETL):
    """
    Test class to perform the ETL and analysis for capacity analysis for FttX. Will not write records to the firestore.
    """
    ...
