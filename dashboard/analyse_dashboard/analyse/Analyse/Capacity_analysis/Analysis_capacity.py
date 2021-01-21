from Analyse.Capacity_analysis.PhaseCapacity.GeulenCapacity import GeulenCapacity
from Analyse.Capacity_analysis.PhaseCapacity.LasAPCapacity import LasAPCapacity
from Analyse.Capacity_analysis.PhaseCapacity.LasDPCapacity import LasDPCapacity
from Analyse.Capacity_analysis.PhaseCapacity.OpleverCapacity import OpleverCapacity
from Analyse.Capacity_analysis.PhaseCapacity.SchietenCapacity import SchietenCapacity
from Analyse.ETL import Extract, Load
from Analyse.FttX import FttXTestLoad, PickleExtract, FttXExtract, FttXTransform
from datetime import timedelta
import pandas as pd

from Analyse.Record.RecordList import RecordList


# TODO: Documentation by Casper van Houten
class CapacityExtract(Extract):
    ...


# TODO: Documentation by Casper van Houten
class CapacityTransform(FttXTransform):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not hasattr(self, 'config'):
            self.config = kwargs.get("config")
        self.performance_norm_config = 1

    def transform(self):
        super().transform()
        self.fill_projectspecific_phase_config()

    def fill_projectspecific_phase_config(self):
        phases_projectspecific = {}
        # Temporarily hard-coded values
        performance_norm_config = 1
        default_total_unit_dict = dict(total_meters_bis=1000, total_meters_tuinschieten=500, total_number_huisaansluitingen=5000)
        default_start_date = pd.to_datetime('2021-01-01')
        for project in self.transformed_data.df.project.unique():
            phases_projectspecific[project] = {}
            for phase, phase_config in self.config['capacity_phases'].items():
                project_info = self.extracted_data.project_info[project]

                # Temporary default value getters as data are incomplete for now.
                civiel_startdatum = project_info.get('civiel_startdatum', default_start_date)
                total_units = project_info.get(phase_config['units_key'],
                                               default_total_unit_dict[phase_config['units_key']])
                phases_projectspecific[project][phase] = \
                    dict(start_date=civiel_startdatum + timedelta(days=phase_config['phase_delta']),
                         total_units=total_units,
                         performance_norm_unit=self.performance_norm_config / 100 * total_units,
                         phase_column=phase_config['phase_column'],
                         n_days=(100 / performance_norm_config - 1)
                         )
        self.transformed_data.project_phase_data = phases_projectspecific


# TODO: Documentation by Casper van Houten
class CapacityLoad(Load):
    def load(self):
        self.record_list.to_firestore()


# TODO: Casper van Houten, add some simple examples
class CapacityAnalyse:
    """
    Main class for the analyses required for the capacity planning algorithm. Per project and phase,
    required indicators are calculated and stored in dedicated phase objects. At the moment,
    the parameters performance_norm_config, n_days_config, phases_config, civil_date, total_units and
    phases_projectspecific are defined here but will be moved to config or transform in the following ticket.

    Example:
    ========
    >>> pass

    """
    def analyse(self):
        self.get_lines_per_phase()

    # TODO: Capser van Houten, refactor this method. The name starts with get, this suggests that the method returns
    #  something.
    def get_lines_per_phase(self):
        """
        Main loop to make capacity objects for all projects. Will fill record dict with LineRecord objects.
        """
        line_record_list = RecordList()
        for project, project_df in self.transformed_data.df.groupby(by="project"):
            phase_data = self.transformed_data.project_phase_data[project]
            line_record_list += GeulenCapacity(df=self.transformed_data.df[self.phases_config['geulen']['phase_column']],
                                               phases_config=phase_data['geulen'],  # Example phase_data.
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


class CapacityETL(FttXExtract, CapacityTransform, CapacityAnalyse):
    """
    Main class to perform the ETL and analysis for capacity analysis for FttX. Will write records to the firestore.
    """
    ...


class CapacityTestETL(PickleExtract, FttXTestLoad, CapacityETL):
    """
    Test class to perform the ETL and analysis for capacity analysis for FttX. Will not write records to the firestore.
    """
    ...
