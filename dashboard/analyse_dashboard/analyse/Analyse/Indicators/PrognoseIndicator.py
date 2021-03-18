import business_rules as br
from Analyse.Indicators.LineIndicator import LineIndicator
from Analyse.Capacity_analysis.Line import TimeseriesLine
from Analyse.Capacity_analysis.Domain import DateDomainRange
from Analyse.Indicators.RealisationIndicator import RealisationIndicator


class PrognoseIndicator(LineIndicator):
    def __init__(self, df, **kwargs):
        super().__init__(**kwargs)
        self.type_start_date = 'FTU0'
        self.type_total_amount = 'huisaansluitingen'
        self.indicator_name = 'PrognoseIndicator'
        self.df = df
        self.mean_realisation_rate_client = self._calculate_mean_realisation_rate_client()

    def _make_project_line(self, project):
        # TODO: deze functie straks vervangen met realisatieindicator
        realisation_rate_line = RealisationIndicator(project=project,
                                                     project_info=self.project_info,
                                                     df=self.df,
                                                     client=self.client).perform().line_project
        if realisation_rate_line:
            if len(realisation_rate_line.data) >= 2:
                mean_rate, _ = realisation_rate_line.integrate().linear_regression(data_partition=0.5)
            else:
                mean_rate = self.mean_realisation_rate_client
            start_date = realisation_rate_line.domain.end
            distance_to_max_value = realisation_rate_line.distance_to_max_value()
            if distance_to_max_value:
                n_days = distance_to_max_value / mean_rate
                n_days_int = int(n_days)
                domain = DateDomainRange(begin=start_date, n_days=n_days_int)
                # small correction so that the predicted amount == total amount on the last day
                mean_rate_corrected = mean_rate + (n_days - n_days_int) * mean_rate / n_days_int
                line = realisation_rate_line.append(TimeseriesLine(data=mean_rate_corrected,
                                                                   domain=domain), skip=1)
                line.name = self.indicator_name
                line.max_value = realisation_rate_line.max_value
                line.project = realisation_rate_line.project
            else:
                line = realisation_rate_line
        else:
            start_date = self.project_info[project][self.type_start_date]
            total_amount = self.project_info[project][self.type_total_amount]
            mean_rate = self.mean_realisation_rate_client
            if start_date and total_amount:
                n_days = total_amount / mean_rate
                n_days_int = int(n_days)
                domain = DateDomainRange(begin=start_date, n_days=n_days_int - 1)
                # small correction so that the predicted amount == total amount on the last day
                mean_rate_corrected = mean_rate + (n_days - n_days_int) * mean_rate / n_days_int
                line = TimeseriesLine(data=mean_rate_corrected,
                                      domain=domain,
                                      name=self.indicator_name,
                                      max_value=total_amount,
                                      project=project)
            else:
                line = None
        return line

    def _calculate_mean_realisation_rate_client(self):
        df = self.df[br.hpend(self.df)]
        return df.groupby(['project', 'opleverdatum']).size().mean()
