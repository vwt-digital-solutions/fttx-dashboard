from datetime import datetime, timedelta

from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Indicators.ActualIndicator import ActualIndicator
from Analyse.Indicators.InternalTargetHPendIndicator import \
    InternalTargetHPendIndicator
from Analyse.Indicators.RealisationHPendIndicator import \
    RealisationHPendIndicator
from Analyse.Indicators.WerkvoorraadIndicator import WerkvoorraadIndicator
from Analyse.Record.Record import Record


class PerformanceGraphIndicator(ActualIndicator, Aggregator):
    def __init__(self, project_info, **kwargs):
        """
        Calculates x and y coordinates for every project that has all necessary information available, and makes
        them into a record.

        Args:
            project_info: Project info from clients transformed data
            **kwargs: Args passed to parent indicator object, including dataframe and client
        """
        super().__init__(**kwargs)
        self.project_info = project_info

    def apply_business_rules(self):
        """
        Retrieves aggregates frames from two indicator types, werkvoorraad and realisatie HPend.

        Returns: Two dataframes, werkvoorraad and realisatie.
        """

        werkvoorraad_indicator = WerkvoorraadIndicator(df=self.df, client=self.client)
        werkvoorraad = werkvoorraad_indicator.aggregate(
            werkvoorraad_indicator.apply_business_rules()
        )

        realisatie_indicator = RealisationHPendIndicator(
            df=self.df, project_info=self.project_info, client=self.client
        )
        realisatie = realisatie_indicator.aggregate(
            realisatie_indicator.apply_business_rules()
        )

        return werkvoorraad, realisatie

    def to_record(self, record_dict):
        return Record(
            record=record_dict,
            collection="Indicators",
            client=self.client,
            graph_name="performance_graph",
        )

    def perform(self):
        werkvoorraad, realised = self.apply_business_rules()
        target_indicator = InternalTargetHPendIndicator(
            project_info=self.project_info, client="kpn"
        )
        record_dict = {}
        for project, series in werkvoorraad.iterrows():
            target_line = self.get_target(project, target_indicator)
            if project in realised.index and target_line and series[0] > 0:
                project_dict = {}
                target_series = target_line.integrate().make_series()
                target_ideal = target_series.iloc[9]
                total_units = target_series.iloc[-1]

                realised_number = self.get_realised_number(realised, project)
                percentage_realised = realised_number / total_units

                target_number = self.get_target_number(target_series)
                percentage_target = target_number / total_units

                project_dict["x"] = (percentage_realised - percentage_target) * 100
                project_dict["y"] = series[0] / target_ideal * 100
                record_dict[project] = project_dict

        return self.to_record(record_dict)

    @staticmethod
    def get_target(project, indicator):
        try:
            target = indicator._make_project_line(project)
        except KeyError:
            target = None
        return target

    @staticmethod
    def get_realised_number(series, project):
        this_week = (datetime.now() - timedelta(datetime.now().weekday())).strftime(
            "%Y-%m-%d"
        )
        try:
            project_series = series.loc[project]
        except KeyError:
            print(f"No data for project {project}")
            return None
        try:
            number = project_series.loc[this_week]
        except KeyError:
            number = project_series.iloc[-1]
        return number

    @staticmethod
    def get_target_number(project_series):
        this_week = (datetime.now() - timedelta(datetime.now().weekday())).strftime(
            "%Y-%m-%d"
        )
        if this_week in project_series.index:
            number = project_series.loc[this_week]
        else:
            number = project_series.iloc[-1]
        return number
