import os

from Analyse.Capacity_analysis.PhaseCapacity.GeulenCapacity import GeulenCapacity
from Analyse.Capacity_analysis.PhaseCapacity.LasAPCapacity import LasAPCapacity
from Analyse.Capacity_analysis.PhaseCapacity.LasDPCapacity import LasDPCapacity
from Analyse.Capacity_analysis.PhaseCapacity.OpleverCapacity import OpleverCapacity
from Analyse.Capacity_analysis.PhaseCapacity.SchietenCapacity import SchietenCapacity
from Analyse.ETL import Load, logger
from Analyse.FttX import FttXTestLoad, PickleExtract, FttXTransform, FttXExtract
from Analyse.BIS_ETL import BISETL
from datetime import timedelta
import pandas as pd

from Analyse.Record.RecordList import RecordList


# TODO: Documentation by Casper van Houten
class CapacityExtract(FttXExtract):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = kwargs.get('client')
        self.bis_etl = BISETL(client=self.client)

    def extract(self):
        super().extract()
        self.extract_BIS()

    def extract_BIS(self):
        self.bis_etl.extract()


class CapacityPickleExtract(PickleExtract, CapacityExtract):

    def extract(self):
        super().extract()
        self.extract_BIS()


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
        self.transform_bis_etl()
        self.add_bis_etl_to_transformed_data()

    def transform_bis_etl(self):
        self.bis_etl.transform()

    def add_bis_etl_to_transformed_data(self):
        self.transformed_data.bis = self.bis_etl.transformed_data

    def get_civiel_start_date(self, start_date):
        default_start_date = '2021-01-01'
        if start_date == 'None' or start_date is None:
            start_date = default_start_date
        return pd.to_datetime(start_date)

    def get_total_units(self, total_units, type_total):
        default_total_unit_dict = {'meters BIS': 100000, 'meters tuinschieten': 50000, 'huisaansluitingen': 10000}
        if total_units == 'None' or total_units is None:
            total_units = default_total_unit_dict[type_total]
        return float(total_units)

    def fill_projectspecific_phase_config(self):
        phases_projectspecific = {}
        # Temporarily hard-coded values
        performance_norm_config = 0.38  # based on dates Nijmegen Dukenburg
        # values for Spijkernisse for the moment

        for project in self.transformed_data.df.project.unique():
            phases_projectspecific[project] = {}
            for phase, phase_config in self.config['capacity_phases'].items():
                project_info = self.extracted_data.project_info[project]
                # Temporary default value getters as data are incomplete for now.
                civiel_startdatum = self.get_civiel_start_date(project_info.get('Civiel startdatum'))
                total_units = self.get_total_units(project_info.get(phase_config['units_key']), phase_config['units_key'])
                if pd.isnull(total_units):
                    total_units = 0
                phases_projectspecific[project][phase] = \
                    dict(start_date=civiel_startdatum + timedelta(days=phase_config['phase_delta']),
                         total_units=total_units,
                         performance_norm_unit=self.performance_norm_config / 100 * total_units,
                         phase_column=phase_config['phase_column'],
                         n_days=(100 / performance_norm_config - 1),
                         master_phase=phase_config['master_phase'],
                         phase_norm=phase_config['phase_norm'],
                         phase_delta=phase_config['phase_delta']
                         )
            phases_projectspecific[project]['schieten']['phase_norm'] = 13 / \
                phases_projectspecific[project]['oplever']['total_units'] * \
                phases_projectspecific[project]['schieten']['total_units']
        self.transformed_data.project_phase_data = phases_projectspecific


# TODO: Documentation by Casper van Houten
class CapacityLoad(Load):
    def load(self):
        self.records.to_firestore()


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

        project_mapping = {'KPN Spijkernisse': 'Spijkenisse',
                           'KPN Gouda Kort Haarlem en Noord': 'Gouda Kort-Haarlem',
                           'Nijmegen Dukenburg': 'Dukenburg Schade'
                           }

        line_record_list = RecordList()

        for project, project_df in self.transformed_data.df.groupby(by="project"):
            if not self.transformed_data.df[self.transformed_data.df.project == project].empty:
                if project in project_mapping:
                    project_t = project_mapping[project]
                else:
                    project_t = project_mapping['KPN Spijkernisse']
                dft = self.transformed_data.bis.df
                df_geul = dft[(~dft.meters_bis_geul.isna())].loc[project_t]
                df_schieten = dft[(~dft.meters_tuinboring.isna())].loc[project_t]
                phase_data = self.transformed_data.project_phase_data[project]
                fase_geulen = GeulenCapacity(df=df_geul[phase_data['geulen']['phase_column']],
                                             phases_config=phase_data['geulen'],  # Example phase_data.
                                             phase='geulen',
                                             client=self.client,
                                             project=project
                                             ).algorithm()
                line_record_list += fase_geulen.get_record()
                fase_schieten = SchietenCapacity(df=df_schieten[phase_data['schieten']['phase_column']],
                                                 phases_config=phase_data['schieten'],
                                                 phase='schieten',
                                                 client=self.client,
                                                 project=project,
                                                 werkvoorraad=fase_geulen.poc_ideal.translate_x(
                                                    delta=phase_data['schieten']['phase_delta'])
                                                 ).algorithm()
                line_record_list += fase_schieten.get_record()
                fase_lasap = LasAPCapacity(df=self.transformed_data.df[phase_data['lasap']['phase_column']],
                                           phases_config=phase_data['lasap'],
                                           phase='lasap',
                                           client=self.client,
                                           project=project,
                                           werkvoorraad=fase_geulen.poc_ideal.translate_x(delta=phase_data['lasap']['phase_delta'])
                                           ).algorithm()
                line_record_list += fase_lasap.get_record()
                fase_lasdp = LasDPCapacity(df=self.transformed_data.df[phase_data['lasdp']['phase_column']],
                                           phases_config=phase_data['lasdp'],
                                           phase='lasdp',
                                           client=self.client,
                                           project=project,
                                           werkvoorraad=fase_geulen.poc_ideal.translate_x(delta=phase_data['lasdp']['phase_delta'])
                                           ).algorithm()
                line_record_list += fase_lasdp.get_record()
                fase_oplever = OpleverCapacity(df=self.transformed_data.df[phase_data['oplever']['phase_column']],
                                               phases_config=phase_data['oplever'],
                                               phase='oplever',
                                               client=self.client,
                                               project=project,
                                               werkvoorraad=fase_lasdp.poc_ideal.translate_x(delta=phase_data['oplever']['phase_delta'] -
                                                                                             phase_data['lasdp']['phase_delta'])
                                               ).algorithm()
                line_record_list += fase_oplever.get_record()
            self.records = line_record_list


class CapacityETL(CapacityExtract, CapacityTransform, CapacityAnalyse, CapacityLoad):
    """
    Main class to perform the ETL and analysis for capacity analysis for FttX. Will write records to the firestore.
    """
    def __init__(self, **kwargs):
        self.client = kwargs.get("client", "client_unknown")
        super().__init__(**kwargs)


class CapacityLocalETL(CapacityPickleExtract, CapacityETL):

    def load(self):
        if 'FIRESTORE_EMULATOR_HOST' in os.environ:
            logger.info("Loading into emulated firestore")
            super().load()
        else:
            logger.warning(
                "Attempting to load with a local ETL process but no emulator is configured. Loading aborted.")


class CapacityTestETL(CapacityPickleExtract, FttXTestLoad, CapacityETL):
    """
    Test class to perform the ETL and analysis for capacity analysis for FttX. Will not write records to the firestore.
    """
    ...


class CapacityPickleETL(CapacityPickleExtract, CapacityETL):
    ...
