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
        if start_date == 'None' or start_date is None or start_date == '':
            start_date = default_start_date
        return pd.to_datetime(start_date)

    def get_total_units(self, total_units, type_total):
        default_total_unit_dict = {'meters BIS': 100000, 'meters tuinschieten': 50000, 'huisaansluitingen': 10000}
        if total_units == 'None' or total_units is None or total_units == '':
            total_units = default_total_unit_dict[type_total]
        return float(total_units)

    def fill_projectspecific_phase_config(self):
        phases_projectspecific = {}
        # Temporarily hard-coded values
        performance_norm_config = 0.38  # based  on dates Nijmegen Dukenburg
        # values for Spijkernisse for the moment

        for project in self.transformed_data.df.project.unique():
            phases_projectspecific[project] = {}
            for phase, phase_config in self.config['capacity_phases'].items():
                project_info = self.extracted_data.project_info[project]
                # Temporary default value getters as data are incomplete for now.
                civiel_startdatum = self.get_civiel_start_date(project_info.get('Civiel startdatum'))
                total_units = self.get_total_units(project_info.get(phase_config['units_key']), phase_config['units_key'])
                if pd.isnull(total_units):
                    total_units = 1000
                phases_projectspecific[project][phase] = \
                    dict(start_date=civiel_startdatum + timedelta(days=phase_config['phase_delta']),
                         total_units=total_units,
                         performance_norm_unit=performance_norm_config / 100 * total_units,
                         phase_column=phase_config['phase_column'],
                         n_days=(100 / performance_norm_config - 1),
                         master_phase=phase_config['master_phase'],
                         phase_norm=phase_config['phase_norm'],
                         phase_delta=phase_config['phase_delta'],
                         name=phase_config['name'],
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
        self.lines2record()

    # TODO: Capser van Houten, refactor this method. The name starts with get, this suggests that the method returns
    #  something.
    def lines2record(self):
        """
        Main loop to make capacity objects for all projects. Will fill record dict with LineRecord objects.
        """

        demo_projects = {'KPN Spijkernisse': 'Spijkenisse',
                         'KPN Gouda Kort Haarlem en Noord': 'Gouda Kort-Haarlem',
                         'Nijmegen Dukenburg': 'Dukenburg Schade'
                         }

        line_record_list = RecordList()
        for project, df_woningen in self.transformed_data.df.groupby(by='project'):
            if project in demo_projects:
                phase_data = self.transformed_data.project_phase_data[project]
                df_meters = self.transformed_data.bis.df.loc[demo_projects[project]]
                werkvoorraad = {}

                geulen_obj = GeulenCapacity(df=df_meters,
                                            phase_data=phase_data['geulen'],
                                            client=self.client,
                                            project=project,
                                            ).algorithm()
                werkvoorraad['geulen'] = geulen_obj.pocideal2object().make_series()
                line_record_list += geulen_obj.get_record()

                schieten_obj = SchietenCapacity(df=df_meters,
                                                phase_data=phase_data['schieten'],
                                                masterphase_data=phase_data[phase_data['schieten']['master_phase']],
                                                client=self.client,
                                                project=project,
                                                werkvoorraad=werkvoorraad[phase_data['schieten']['master_phase']],
                                                ).algorithm()
                werkvoorraad['schieten'] = schieten_obj.pocideal2object().make_series()
                line_record_list += schieten_obj.get_record()

                lasap_obj = LasAPCapacity(df=df_woningen,
                                          phase_data=phase_data['lasap'],
                                          masterphase_data=phase_data[phase_data['lasap']['master_phase']],
                                          client=self.client,
                                          project=project,
                                          werkvoorraad=werkvoorraad[phase_data['lasap']['master_phase']],
                                          ).algorithm()
                line_record_list += lasap_obj.get_record()
                werkvoorraad['lasap'] = lasap_obj.pocideal2object().make_series()

                lasdp_obj = LasDPCapacity(df=df_woningen,
                                          phase_data=phase_data['lasdp'],
                                          masterphase_data=phase_data[phase_data['lasdp']['master_phase']],
                                          client=self.client,
                                          project=project,
                                          werkvoorraad=werkvoorraad[phase_data['lasdp']['master_phase']],
                                          ).algorithm()
                werkvoorraad['lasdp'] = lasdp_obj.pocideal2object().make_series()
                line_record_list += lasdp_obj.get_record()

                oplever_obj = OpleverCapacity(df=df_woningen,
                                              phase_data=phase_data['oplever'],
                                              masterphase_data=phase_data[phase_data['oplever']['master_phase']],
                                              client=self.client,
                                              project=project,
                                              werkvoorraad=werkvoorraad[phase_data['oplever']['master_phase']],
                                              ).algorithm()
                werkvoorraad['oplever'] = oplever_obj.pocideal2object().make_series()
                line_record_list += oplever_obj.get_record()

            self.records = line_record_list

    def get_rest_dates_as_list_of_series(self):
        rest_dates = []
        for rest_date in self.config['rest_periods']:
            rest_dates.append(pd.date_range(start=rest_date[0], end=rest_date[1], freq='D'))
        return rest_dates


class CapacityETL(CapacityExtract, CapacityTransform, CapacityAnalyse, CapacityLoad):
    """
    Main class to perform the ETL and analysis for capacity analysis for FttX. Will write records to the firestore.
    """
    def __init__(self, **kwargs):
        self.client = kwargs.get("client", "client_unknown")
        super().__init__(**kwargs)

    def perform(self):
        self.extract()
        self.transform()
        self.analyse()
        self.load()


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
