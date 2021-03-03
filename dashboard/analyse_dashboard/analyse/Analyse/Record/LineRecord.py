from Analyse.Record.Record import Record
from functions import get_timestamp_of_period
from toggles import ReleaseToggles

toggles = ReleaseToggles('toggles.yaml')


# TODO: Documentation by Casper van Houten
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
    if toggles.transform_line_record:
        def __init__(self, record, collection, client, graph_name, phase, project, resample_method='sum',
                     to_be_integrated=True, to_be_normalized=True, percentage=True, **kwargs):
            self.phase = phase
            self.project = project
            self.resample_method = resample_method
            self.to_be_integrated = to_be_integrated
            self.to_be_normalized = to_be_normalized
            self.percentage = percentage
            super().__init__(record, collection, client, graph_name, **kwargs)
    else:
        def __init__(self, record, collection, client, graph_name, phase, project, **kwargs):
            super().__init__(record, collection, client, graph_name, **kwargs)
            self.phase = phase
            self.project = project

    def _to_document(self):
        return dict(record=self.record,
                    client=self.client,
                    line=self.graph_name,
                    project=self.project,
                    phase=self.phase)

    def _transform(self, record):
        """This functions transforms the line object in the record to the desired aggregate for output.

        Args:
            record (object): object of a line

        Returns:
            record (dict): dictionary of aggregates required for dashboard.
        """

        if toggles.transform_line_record:
            record_to_write = dict()
            record_to_write['configuration'] = {
                'resample method': self.resample_method,
                'integrated': self.to_be_integrated,
                'normalized': self.to_be_normalized,
                'percentage': self.percentage
            }
            line = record
            line_week = line.resample(freq='W-MON', method=self.resample_method, loffset='-1')
            line_month = line.resample(freq='MS', method=self.resample_method, loffset='-1')

            last_week = get_timestamp_of_period(freq='W-MON', period='last_period')
            record_to_write['last_week'] = line_week.make_series()[
                last_week] if last_week in line_week.make_series().index else 0

            current_week = get_timestamp_of_period(freq='W-MON', period='current_period')
            record_to_write['current_week'] = line_week.make_series()[
                current_week] if current_week in line_week.make_series().index else 0

            next_week = get_timestamp_of_period(freq='W-MON', period='next_period')
            record_to_write['next_week'] = line_week.make_series()[
                next_week] if next_week in line_week.make_series().index else 0

            last_month = get_timestamp_of_period(freq='MS', period='last_period')
            record_to_write['last_month'] = line_month.make_series()[
                last_month] if last_month in line_month.make_series().index else 0

            current_month = get_timestamp_of_period(freq='MS', period='current_period')
            record_to_write['current_month'] = line_month.make_series()[
                current_month] if current_month in line_month.make_series().index else 0

            next_month = get_timestamp_of_period(freq='MS', period='next_period')
            record_to_write['next_month'] = line_month.make_series()[
                next_month] if next_month in line_month.make_series().index else 0

            if self.to_be_integrated:
                line_week = line_week.integrate()
                line_month = line_month.integrate()

            if self.to_be_normalized:
                series_week = line_week.make_normalised_series(percentage=self.percentage)
                series_month = line_month.make_normalised_series(percentage=self.percentage)
            else:
                series_week = line_week.make_series()
                series_month = line_month.make_series()

            series_week.index = series_week.index.format()
            record_to_write['series_week'] = series_week.to_dict()
            series_month.index = series_month.index.format()
            record_to_write['series_month'] = series_month.to_dict()

            return record_to_write

        else:
            record_to_write = dict()
            record_to_write['series_week'] = record.get_line_aggregate(freq='W-MON',
                                                                       loffset='-1',
                                                                       aggregate_type='series',
                                                                       index_as_str=True).to_dict()
            record_to_write['series_month'] = record.get_line_aggregate(freq='MS',
                                                                        aggregate_type='series',
                                                                        index_as_str=True).to_dict()
            if record.name == 'work_stock_amount_indicator':
                record_to_write['next_week'] = record.get_line_aggregate(freq='W-MON',
                                                                         loffset='-1',
                                                                         aggregate_type='value_mean')
                record_to_write['next_month'] = record.get_line_aggregate(freq='MS',
                                                                          aggregate_type='value_mean')
            else:
                record_to_write['next_week'] = record.get_line_aggregate(freq='W-MON',
                                                                         loffset='-1',
                                                                         aggregate_type='value_sum')
                record_to_write['next_month'] = record.get_line_aggregate(freq='MS',
                                                                          aggregate_type='value_sum')
            return record_to_write

    def document_name(self, **kwargs):
        """
        Make document name based on client, phase and graph name.
        Args:
            **kwargs:

        Returns
            str: Document name as string.

        """

        return f'{self.client}_{self.project}_{self.phase}_{self.graph_name}'
