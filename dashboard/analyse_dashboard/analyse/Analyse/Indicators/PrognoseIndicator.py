import math

import business_rules as br
from Analyse.Capacity_analysis.Domain import DateDomainRange
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Indicators.LineIndicator import LineIndicator
from Analyse.Indicators.RealisationHPendIndicator import \
    RealisationHPendIndicator


class PrognoseIndicator(LineIndicator):
    def __init__(self, df, **kwargs):
        super().__init__(**kwargs)
        self.type_start_date = "FTU0"
        self.type_total_amount = "huisaansluitingen"
        self.indicator_name = "PrognoseHPendIndicator"
        self.df = df
        self.mean_realisation_rate_client = (
            self._calculate_mean_realisation_rate_client()
        )

    def _make_project_line(self, project):
        """Creates a line for given project that extrapolates the daily speed of realisation line (HPend / day)
        that has been achieved so far. The extrapolation is based on the average of a set of daily speeds
        in the realisation line (on default the set of speeds at the last 50% of days on which HPend has been realized).
        If too little has been realized on the project for calculation of an average, the extrapolation
        is based on the average speed over all projects.

        Args:
            project

        Returns:
            Timeseries Line
        """
        start_date = self.project_info[project][self.type_start_date]
        total_amount = self.project_info[project][self.type_total_amount]
        realisation_rate_line = RealisationHPendIndicator(
            project_info=self.project_info,
            df=self.df[self.df.project == project],
            client=self.client,
            return_lines=True,
            silence=True,
        ).perform()
        if realisation_rate_line:
            realisation_rate_line = realisation_rate_line[0]
        else:
            realisation_rate_line = None
        mean_rate = self.mean_realisation_rate_client
        if realisation_rate_line:
            if len(realisation_rate_line) >= 2:
                mean_rate, _ = realisation_rate_line.integrate().linear_regression(
                    data_partition=0.5
                )

        if realisation_rate_line and total_amount:
            extrapolated_rate_line = self._make_extrapolated_line(
                realisation_rate_line, mean_rate, total_amount
            )
            line = realisation_rate_line.append(
                other=extrapolated_rate_line,
                skip=1,
                name=self.indicator_name,
                max_value=total_amount,
                project=project,
            )
        elif realisation_rate_line and not total_amount:
            line = realisation_rate_line
        elif not realisation_rate_line and start_date and total_amount:
            n_days = total_amount / mean_rate
            n_days_int = math.floor(n_days)
            domain = DateDomainRange(begin=start_date, n_days=n_days_int - 1)
            # small correction so that the predicted amount == total amount on the last day
            mean_rate_corrected = (
                mean_rate + (n_days - n_days_int) * mean_rate / n_days_int
            )
            line = TimeseriesLine(
                data=mean_rate_corrected,
                domain=domain,
                name=self.indicator_name,
                max_value=total_amount,
                project=project,
            )
        else:
            line = None
        return line

    def _calculate_mean_realisation_rate_client(self):
        """Calculates the average daily speed of realisation (HPend / day) over all projects.

        Returns:
            Value of average speed
        """
        df = self.df[br.hpend(self.df)]
        return df.groupby(["project", "opleverdatum"]).size().mean()

    def _make_extrapolated_line(self, realisation_rate_line, mean_rate, total_amount):
        """Extrapolates the realised timeseriesline with the average speed until the end of the project is reached.

        Args:
            realisation_rate_line: realised timeseries line calculated with the RealisationHPendIndicator.
            mean_rate: the average speed for extrapolation.
            total_amount: the total number of houses to be realised in the project.

        Returns:
            Extrapolated timeseries line
        """
        start_date = realisation_rate_line.domain.end
        distance_to_max_value = (
            total_amount - realisation_rate_line.integrate().get_most_recent_point()
        )
        n_days = distance_to_max_value / mean_rate
        if n_days >= 1:
            n_days_int = math.floor(n_days)
            # small correction so that the predicted amount == total amount on the last day
            mean_rate_corrected = (
                mean_rate + (n_days - n_days_int) * mean_rate / n_days_int
            )
        else:
            n_days_int = 1
            mean_rate_corrected = mean_rate * n_days

        domain = DateDomainRange(begin=start_date, n_days=n_days_int)
        line = TimeseriesLine(data=mean_rate_corrected, domain=domain)
        return line
