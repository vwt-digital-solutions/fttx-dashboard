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
import copy

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
        logger.info("Transforming the data following the Capacity protocol")
        logger.info("Transforming by using the extracted data directly. There was no previous tranformed data")
        self.transformed_data = copy.deepcopy(self.extracted_data)
        self._fix_dates()
        self._clean_ftu_data()
        self.fill_projectspecific_phase_config()
        self.transform_bis_etl()
        self.add_bis_etl_to_transformed_data()
        self._combine_and_reformat_data_for_capacity_analysis()

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

    def _combine_and_reformat_data_for_capacity_analysis(self):
        self.dict_capacity = {}
        demo_projects = self.config['demo_projects_capacity']
        for project in demo_projects:
            df_capacity_project = pd.DataFrame()
            for phase_config in self.config['capacity_phases'].values():
                ds_add = pd.Series()
                if phase_config['phase_column'] in self.transformed_data.bis.df.columns:
                    ds_add = self.transformed_data.bis.df.loc[demo_projects[project]][phase_config['phase_column']]
                    ds_add = ds_add[~ds_add.isna()]
                if phase_config['phase_column'] in self.transformed_data.df.columns:
                    ds_add = self.transformed_data.df[phase_config['phase_column']]
                    ds_add = ds_add[(~ds_add.isna()) & (ds_add <= pd.Timestamp.now())]
                    ds_add = ds_add.groupby(ds_add.dt.date).count()
                    ds_add.index.name = 'date'
                df_capacity_project = df_capacity_project.add(pd.DataFrame(ds_add), fill_value=0).fillna(0)
            self.dict_capacity[project] = df_capacity_project


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
        self.lines_to_record()

    # TODO: Capser van Houten, refactor this method. The name starts with get, this suggests that the method returns
    #  something.
    def lines_to_record(self):
        """
        Main loop to make capacity objects for all projects. Will fill record dict with LineRecord objects.
        """

        line_record_list = RecordList()
        for project in self.config['demo_projects_capacity']:
            phase_data = self.transformed_data.project_phase_data[project]
            df = self.dict_capacity[project]
            pocideal_line = {}

            geulen = GeulenCapacity(df=df,
                                    phase_data=phase_data['geulen'],
                                    client=self.client,
                                    project=project,
                                    ).algorithm()
            pocideal_line['geulen'] = geulen.calculate_pocideal_line()
            line_record_list += geulen.get_record()

            schieten = SchietenCapacity(df=df,
                                        phase_data=phase_data['schieten'],
                                        masterphase_data=phase_data[phase_data['schieten']['master_phase']],
                                        client=self.client,
                                        project=project,
                                        pocideal_line_masterphase=pocideal_line[phase_data['schieten']['master_phase']],
                                        ).algorithm()
            pocideal_line['schieten'] = schieten.calculate_pocideal_line()
            line_record_list += schieten.get_record()

            lasap = LasAPCapacity(df=df,
                                  phase_data=phase_data['lasap'],
                                  masterphase_data=phase_data[phase_data['lasap']['master_phase']],
                                  client=self.client,
                                  project=project,
                                  pocideal_line_masterphase=pocideal_line[phase_data['lasap']['master_phase']],
                                  ).algorithm()
            line_record_list += lasap.get_record()
            pocideal_line['lasap'] = lasap.calculate_pocideal_line()

            lasdp = LasDPCapacity(df=df,
                                  phase_data=phase_data['lasdp'],
                                  masterphase_data=phase_data[phase_data['lasdp']['master_phase']],
                                  client=self.client,
                                  project=project,
                                  pocideal_line_masterphase=pocideal_line[phase_data['lasdp']['master_phase']],
                                  ).algorithm()
            pocideal_line['lasdp'] = lasdp.calculate_pocideal_line()
            line_record_list += lasdp.get_record()

            oplever = OpleverCapacity(df=df,
                                      phase_data=phase_data['oplever'],
                                      masterphase_data=phase_data[phase_data['oplever']['master_phase']],
                                      client=self.client,
                                      project=project,
                                      pocideal_line_masterphase=pocideal_line[phase_data['oplever']['master_phase']],
                                      ).algorithm()
            pocideal_line['oplever'] = oplever.calculate_pocideal_line()
            line_record_list += oplever.get_record()

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
