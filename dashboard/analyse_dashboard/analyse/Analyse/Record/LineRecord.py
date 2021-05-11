import pandas as pd
from dateutil.relativedelta import relativedelta

from Analyse.Record.Record import Record
from functions import get_timestamp_of_period, round_
from toggles import ReleaseToggles

toggles = ReleaseToggles("toggles.yaml")


class LineRecord(Record):
    """
    Record type that deals specifically with lines.
    Should be able to write different attributes of lines to firestore, given flags in init.

    Args:
        record:
        collection:
        client:
        graph_name:
        phase:
        **kwargs:
    """

    def __init__(
        self,
        record,
        collection,
        client,
        graph_name,
        phase,
        project,
        resample_method="sum",
        to_be_integrated=True,
        to_be_normalized=True,
        percentage=True,
        to_be_splitted_by_year=False,
        **kwargs,
    ):
        self.phase = phase
        self.project = project
        self.resample_method = resample_method
        self.to_be_integrated = to_be_integrated
        self.to_be_normalized = to_be_normalized
        self.percentage = percentage
        self.to_be_splitted_by_year = to_be_splitted_by_year
        self.max_value = record.max_value
        super().__init__(record, collection, client, graph_name, **kwargs)

    def _to_document(self):
        return dict(
            record=self.record,
            client=self.client,
            line=self.graph_name,
            project=self.project,
            phase=self.phase,
        )

    def _transform(self, line):
        """This functions transforms the line object in the record to the desired aggregate for output.

        Args:
            line (TimeseriesLine): object of a line

        Returns:
            record (dict): dictionary of aggregates required for dashboard.
        """

        record_to_write = dict()
        record_to_write["configuration"] = {
            "resample method": self.resample_method,
            "integrated": self.to_be_integrated,
            "normalized": self.to_be_normalized,
            "percentage": self.percentage,
        }

        line_week = line.resample(freq="W-MON", method=self.resample_method)
        line_month = line.resample(freq="MS", method=self.resample_method)
        line_year = line.resample(freq="YS", method=self.resample_method)

        record_to_write["last_week"] = self._get_value_of_period(
            line_week, period="last"
        )
        record_to_write["current_week"] = self._get_value_of_period(
            line_week, period="current"
        )
        record_to_write["next_week"] = self._get_value_of_period(
            line_week, period="next"
        )
        record_to_write["last_month"] = self._get_value_of_period(
            line_month, period="last"
        )
        record_to_write["current_month"] = self._get_value_of_period(
            line_month, period="current"
        )
        record_to_write["next_month"] = self._get_value_of_period(
            line_month, period="next"
        )

        record_to_write["next_4_weeks"] = self.calculate_value_for_next_4_weeks(
            line_week
        )

        record_to_write["max_value_on_weekly_basis"] = int(
            line_week.make_series().max()
        )

        record_to_write["series_week"] = self.configure_series_to_write(line_week)
        record_to_write["series_month"] = self.configure_series_to_write(line_month)
        record_to_write["series_year"] = self.configure_series_to_write(line_year)

        if self.to_be_splitted_by_year:
            lines_week = line_week.split_by_year()
            lines_month = line_month.split_by_year()
            for line in lines_week:
                year = line.get_extreme_period_of_series("year", "max")
                record_to_write[
                    f"series_week_{year}"
                ] = self.configure_series_to_write(line)
            for line in lines_month:
                year = line.get_extreme_period_of_series("year", "min")
                record_to_write[
                    f"series_month_{year}"
                ] = self.configure_series_to_write(line)

        return record_to_write

    def configure_series_to_write(self, line):
        """
        Given a configuration, this function calculates the desired output
        Returns: configured timeseries (dict)

        """
        if self.to_be_integrated:
            line = line.integrate()

        if self.to_be_normalized:
            series = line.make_normalised_series(
                percentage=self.percentage, maximum=self.max_value
            )
        else:
            series = line.make_series()

        series.index = series.index.format()
        return round_(series).to_dict()

    def document_name(self, **kwargs):
        """
        Make document name based on client, phase and graph name.
        Args:
            **kwargs:

        Returns
            str: Document name as string.

        """

        return f"{self.client}_{self.project}_{self.phase}_{self.graph_name}"

    def _get_value_of_period(self, line, period):
        """
        This method returns the value of a given period base on the frequency of the given TimeseriesLine. If
        this period does not exists in the index, the function will return 0
        Args:
            line: TimeseriesLine
            period: last, current or next period to be returned

        Returns: value of last, current or next period, or 0 if this period is not present in the index.
        """
        freq = self._get_freq_from_timeseries(line)
        timestamp = get_timestamp_of_period(freq=freq, period=period)
        return (
            round_(float(line.make_series()[timestamp]))
            if timestamp in line.make_series().index
            else 0
        )

    @staticmethod
    def calculate_value_for_next_4_weeks(line_week):
        """
        This function calculates the sum of the upcomming 4 weeks starting next monday.
        Args:
            line_week (Line): resample weekly W-MON line

        Returns: Work to do for the next upcomming 4 weeks

        """
        weekday = pd.Timestamp.today().weekday()
        timestamp_4_weeks = (
            pd.Timestamp.today() - relativedelta(days=weekday) + relativedelta(weeks=4)
        )
        line_week_series = line_week.make_series()
        value_next_4_weeks = float(
            line_week_series[
                (line_week_series.index > pd.Timestamp.today())
                & (line_week_series.index <= timestamp_4_weeks)
            ].sum()
        )
        return round_(value_next_4_weeks)

    @staticmethod
    def _get_freq_from_timeseries(line):
        """
        Function to return frequency of TimeseriesLine
        Args:
            line: TimeseriesLine

        Returns: frequency (str)

        """
        if "Day" in str(line.data.index.freq):
            freq = "D"
        elif "Week" in str(line.data.index.freq):
            freq = "W-MON"
        elif "Month" in str(line.data.index.freq):
            freq = "MS"
        else:
            raise ValueError(f"The frequency {line.data.index.freq} is not configured")
        return freq
