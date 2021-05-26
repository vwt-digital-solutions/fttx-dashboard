import copy
import os
from datetime import timedelta

import pandas as pd

from Analyse.BIS_ETL import BISETL
from Analyse.Capacity_analysis.PhaseCapacity.GeulenCapacity import \
    GeulenCapacity
from Analyse.Capacity_analysis.PhaseCapacity.LasAPCapacity import LasAPCapacity
from Analyse.Capacity_analysis.PhaseCapacity.LasDPCapacity import LasDPCapacity
from Analyse.Capacity_analysis.PhaseCapacity.OpleverCapacity import \
    OpleverCapacity
from Analyse.Capacity_analysis.PhaseCapacity.SchietenCapacity import \
    SchietenCapacity
from Analyse.ETL import ETLBase, Load, logger
from Analyse.FttX import (FttXExtract, FttXTestLoad, FttXTransform,
                          PickleExtract)
from Analyse.Record.RecordList import RecordList


# TODO: Documentation by Casper van Houten
class CapacityExtract(FttXExtract):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client = kwargs.get("client")
        self.bis_etl = BISETL(client=self.client, config=self.config)

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
        if not hasattr(self, "config"):
            self.config = kwargs.get("config")
        self.performance_norm_config = 1

    def transform(self):
        logger.info("Transforming the data following the Capacity protocol")
        logger.info(
            "Transforming by using the extracted data directly. There was no previous tranformed data"
        )
        self.transformed_data = copy.deepcopy(self.extracted_data)
        self._fix_dates()
        self.fill_projectspecific_phase_config()
        self.transform_bis_etl()
        self.add_bis_etl_to_transformed_data()
        self._combine_and_reformat_data_for_capacity_analysis()

    def transform_bis_etl(self):
        self.bis_etl.transform()

    def add_bis_etl_to_transformed_data(self):
        self.transformed_data.bis = self.bis_etl.transformed_data

    def get_civiel_start_date(self, start_date):
        default_start_date = "2021-01-01"
        if start_date == "None" or start_date is None or start_date == "":
            start_date = default_start_date
        return pd.to_datetime(start_date)

    def get_total_units(self, total_units, type_total):
        default_total_unit_dict = {
            "meters BIS": 100000,
            "meters tuinschieten": 50000,
            "huisaansluitingen": 10000,
        }
        if total_units == "None" or total_units is None or total_units == "":
            total_units = default_total_unit_dict[type_total]
        return float(total_units)

    def fill_projectspecific_phase_config(self):
        phases_projectspecific = {}
        # Temporarily hard-coded values
        performance_norm_config = 0.366  # based  on dates Nijmegen Dukenburg
        # values for Spijkernisse for the moment

        for project in self.transformed_data.df.project.unique():
            phases_projectspecific[project] = {}
            for phase, phase_config in self.config["capacity_phases"].items():
                project_info = self.extracted_data.project_info[project]
                # Temporary default value getters as data are incomplete for now.
                civiel_startdatum = self.get_civiel_start_date(
                    project_info.get("Civiel startdatum")
                )
                total_units = self.get_total_units(
                    project_info.get(phase_config["units_key"]),
                    phase_config["units_key"],
                )
                if pd.isnull(total_units):
                    total_units = 1000
                phases_projectspecific[project][phase] = dict(
                    start_date=civiel_startdatum
                    + timedelta(days=phase_config["phase_delta"]),
                    total_units=total_units,
                    performance_norm_unit=performance_norm_config / 100 * total_units,
                    phase_column=phase_config["phase_column"],
                    n_days=(100 / performance_norm_config - 1),
                    master_phase=phase_config["master_phase"],
                    phase_norm=phase_config["phase_norm"],
                    phase_delta=phase_config["phase_delta"],
                    name=phase_config["name"],
                )
            phases_projectspecific[project]["schieten"]["phase_norm"] = (
                13
                / phases_projectspecific[project]["oplever"]["total_units"]
                * phases_projectspecific[project]["schieten"]["total_units"]
            )
        self.transformed_data.project_phase_data = phases_projectspecific

    def _combine_and_reformat_data_for_capacity_analysis(self):
        """This function collects the required date columns (for each phase) for the capacity analysis,
        reformats them and puts them per project in a dedicated dictionary (dict_capacity).
        """
        self.dict_capacity = {}
        demo_projects = self.config["demo_projects_capacity"]
        for project in demo_projects:
            df_capacity_project = pd.DataFrame()
            for phase_config in self.config["capacity_phases"].values():
                ds_add = pd.Series()
                if phase_config["phase_column"] in self.transformed_data.bis.df.columns:
                    ds_add = self.transformed_data.bis.df.loc[project][
                        phase_config["phase_column"]
                    ]
                    ds_add = ds_add[~ds_add.isna()]
                if phase_config["phase_column"] in self.transformed_data.df.columns:
                    df_project = self.transformed_data.df[
                        self.transformed_data.df.project == project
                    ]
                    ds_add = df_project[phase_config["phase_column"]]
                    ds_add = ds_add[(~ds_add.isna()) & (ds_add <= pd.Timestamp.now())]
                    ds_add = ds_add.groupby(ds_add.dt.date).count()
                    ds_add.index.name = "date"
                df_capacity_project = df_capacity_project.add(
                    pd.DataFrame(ds_add), fill_value=0
                ).fillna(0)
                df_capacity_project.index = pd.to_datetime(df_capacity_project.index)
            self.dict_capacity[project] = df_capacity_project


# TODO: Documentation by Casper van Houten
class CapacityLoad(Load):
    def load(self):
        self.records.to_firestore()


# TODO: Casper van Houten, add some simple examples
class CapacityAnalyse(ETLBase):
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

    def lines_to_record(self):
        """This functions makes the line objects required for the capacity algorithm for the phases geulen,
        tuinschieten, lasap, lasdp and oplever for the projects specified in the list demo_projects_capacity.
        First the required data is collected together with phase configuration data and holiday ranges. Then per phase,
        the phase object is made with the dedicated version of the PhaseCapacity class. As a final step,
        the line records made within the phase object are added to the line_record_list.
        """
        holiday_ranges = self.transform_holiday_dates_into_ranges()
        line_record_list = RecordList()
        for project in self.config["demo_projects_capacity"]:
            phase_data = self.transformed_data.project_phase_data[project]
            df = self.dict_capacity[project]
            poc_ideal_rate_line = {}

            geulen = GeulenCapacity(
                df=df,
                phase_data=phase_data["geulen"],
                client=self.client,
                project=project,
                holiday_periods=holiday_ranges,
            ).algorithm()
            poc_ideal_rate_line["geulen"] = geulen.calculate_poc_ideal_rate_line()
            line_record_list += geulen.get_record()

            schieten = SchietenCapacity(
                df=df,
                phase_data=phase_data["schieten"],
                masterphase_data=phase_data[phase_data["schieten"]["master_phase"]],
                client=self.client,
                project=project,
                holiday_periods=holiday_ranges,
                poc_ideal_rate_line_masterphase=poc_ideal_rate_line[
                    phase_data["schieten"]["master_phase"]
                ],
            ).algorithm()
            poc_ideal_rate_line["schieten"] = schieten.calculate_poc_ideal_rate_line()
            line_record_list += schieten.get_record()

            lasap = LasAPCapacity(
                df=df,
                phase_data=phase_data["lasap"],
                masterphase_data=phase_data[phase_data["lasap"]["master_phase"]],
                client=self.client,
                project=project,
                holiday_periods=holiday_ranges,
                poc_ideal_rate_line_masterphase=poc_ideal_rate_line[
                    phase_data["lasap"]["master_phase"]
                ],
            ).algorithm()
            line_record_list += lasap.get_record()
            poc_ideal_rate_line["lasap"] = lasap.calculate_poc_ideal_rate_line()

            lasdp = LasDPCapacity(
                df=df,
                phase_data=phase_data["lasdp"],
                masterphase_data=phase_data[phase_data["lasdp"]["master_phase"]],
                client=self.client,
                project=project,
                holiday_periods=holiday_ranges,
                poc_ideal_rate_line_masterphase=poc_ideal_rate_line[
                    phase_data["lasdp"]["master_phase"]
                ],
            ).algorithm()
            poc_ideal_rate_line["lasdp"] = lasdp.calculate_poc_ideal_rate_line()
            line_record_list += lasdp.get_record()

            oplever = OpleverCapacity(
                df=df,
                phase_data=phase_data["oplever"],
                masterphase_data=phase_data[phase_data["oplever"]["master_phase"]],
                client=self.client,
                project=project,
                holiday_periods=holiday_ranges,
                poc_ideal_rate_line_masterphase=poc_ideal_rate_line[
                    phase_data["oplever"]["master_phase"]
                ],
            ).algorithm()
            poc_ideal_rate_line["oplever"] = oplever.calculate_poc_ideal_rate_line()
            line_record_list += oplever.get_record()

        self.records = line_record_list

    def transform_holiday_dates_into_ranges(self):
        holiday_ranges = []
        for holiday_periods in self.config["holidays_periods"]:
            holiday_ranges.append(
                pd.date_range(
                    start=holiday_periods[0], end=holiday_periods[1], freq="D"
                )
            )
        return holiday_ranges


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
        if "FIRESTORE_EMULATOR_HOST" in os.environ:
            logger.info("Loading into emulated firestore")
            super().load()
        else:
            logger.warning(
                "Attempting to load with a local ETL process but no emulator is configured. Loading aborted."
            )


class CapacityTestETL(CapacityPickleExtract, FttXTestLoad, CapacityETL):
    """
    Test class to perform the ETL and analysis for capacity analysis for FttX. Will not write records to the firestore.
    """

    ...


class CapacityPickleETL(CapacityPickleExtract, CapacityETL):
    ...
