from datetime import datetime, timedelta

from Analyse.Aggregators.Aggregator import Aggregator
from Analyse.Capacity_analysis.Line import TimeseriesLine
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
        Used in indicator performancegraph

        Args:
            project_info: Project info from clients transformed data
            **kwargs: Args passed to parent indicator object, including dataframe and client
        """
        super().__init__(**kwargs)
        self.project_info = project_info

    def perform(self):
        werkvoorraad, realisatie, target = self.get_instances_of_indicators()
        # get filtered dataframes for werkvoorraad and realisatie
        df_werkvoorraad = werkvoorraad.aggregate(werkvoorraad.apply_business_rules())
        df_realisatie = realisatie.aggregate(realisatie.apply_business_rules())
        # get current week
        this_week = (datetime.now() - timedelta(datetime.now().weekday())).strftime(
            "%Y-%m-%d"
        )

        x = []
        y = []
        names_projects = []
        for project in self.project_info:
            total_units = self.project_info[project]["huisaansluitingen"]
            progress_of_target, werkvoorraad_ideal = self.get_progress_of_target(
                target, project, this_week, total_units
            )
            progress_of_realisatie = self.get_progress_of_realisatie(
                df_realisatie, project, this_week, total_units
            )
            werkvoorraad = self.get_werkvoorraad(df_werkvoorraad, project, total_units)
            if progress_of_target and progress_of_realisatie and werkvoorraad:
                x += [round((progress_of_realisatie - progress_of_target) * 100, 2)]
                y += [round((werkvoorraad / werkvoorraad_ideal) * 100, 2)]
                names_projects += [project]
        return self.to_record(dict(x=x, y=y, names=names_projects))

    def get_instances_of_indicators(self):
        """
        Retrieves aggregates frames from two indicator types, werkvoorraad and realisatie HPend.

        Returns: Two dataframes, werkvoorraad and realisatie.
        """

        # create instances of required indicators
        werkvoorraad = WerkvoorraadIndicator(df=self.df, client=self.client)
        realisatie = RealisationHPendIndicator(
            df=self.df, project_info=self.project_info, client=self.client
        )
        target = InternalTargetHPendIndicator(
            project_info=self.project_info, client="kpn"
        )

        return werkvoorraad, realisatie, target

    def get_progress_of_target(self, target, project, this_week, total_units):
        target_line = target._make_project_line(project)
        if target_line:
            target_series = (
                target_line.resample(freq="W-MON", method="sum")
                .integrate()
                .make_series()
            )
            if this_week in target_series.index:
                progress = target_series.loc[this_week] / total_units
            else:
                progress = target_series.iloc[-1] / total_units
            if len(target_series) >= 8:
                werkvoorraad_ideal = target_series.iloc[7] / total_units
            else:
                werkvoorraad_ideal = target_series.iloc[-1] / total_units
        else:
            progress = None
            werkvoorraad_ideal = None
        return progress, werkvoorraad_ideal

    def get_progress_of_realisatie(
        self, df_realisatie, project, this_week, total_units
    ):
        if project in df_realisatie.index:
            data = df_realisatie.loc[project]
            realisatie_series = (
                TimeseriesLine(data=data)
                .resample(freq="W-MON", method="sum")
                .integrate()
                .make_series()
            )
            if this_week in realisatie_series.index:
                progress = realisatie_series.loc[this_week] / total_units
            else:
                progress = realisatie_series.iloc[-1] / total_units
        else:
            progress = None
        return progress

    def get_werkvoorraad(self, df_werkvoorraad, project, total_units):
        if project in df_werkvoorraad.index and total_units:
            werkvoorraad = df_werkvoorraad.loc[project, "werkvoorraad"] / total_units
        else:
            werkvoorraad = None
        return werkvoorraad

    def to_record(self, record_dict):
        return Record(
            record=record_dict,
            collection="Indicators",
            client=self.client,
            graph_name="data_for_performance_graph",
        )
