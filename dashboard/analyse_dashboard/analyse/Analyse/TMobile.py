from Analyse.FttX import FttXETL, FttXAnalyse, FttXTransform, PickleExtract, FttXTestLoad
from Analyse.Record import Record, DocumentListRecord
from functions_tmobile import calculate_voorraadvormend, add_weeknumber, preprocess_for_jaaroverzicht
from functions_tmobile import counts_by_time_period, calculate_jaaroverzicht
import logging
logger = logging.getLogger('T-mobile Analyse')


class TMobileTransform(FttXTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def transform(self, **kwargs):
        super().transform(**kwargs)
        self._HAS_add_weeknumber()

    def _HAS_add_weeknumber(self):
        self.transformed_data.df['has_week'] = add_weeknumber(self.transformed_data.df['hasdatum'])


class TMobileAnalyse(FttXAnalyse):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def analyse(self):
        super().analyse()
        logger.info("Analysing using the T-mobile protocol")
        self._get_counts_by_month()
        self._get_counts_by_week()
        self._get_voorraadvormend()
        self._jaaroverzicht()

    def _get_voorraadvormend(self):
        logger.info("Calculating voorraadvormend")
        record = calculate_voorraadvormend(self.transformed_data.df)
        self.record_dict.add('voorraadvormend', record, Record, "Data")

    def _get_counts_by_week(self):
        logger.info("Calculating counts by week")
        counts_by_week = counts_by_time_period(self.transformed_data.df)
        drl = [dict(record={k: v},
                    graph_name=f"{k}_by_week")
               for k, v in counts_by_week.items()]
        self.record_dict.add('weekly_date_counts', drl, DocumentListRecord, "Data", document_key=['graph_name'])

    # has_opgeleverd = collection.get_document(collection="Data", graph_name="count_opleverdatum_by_" + period, client=client)
    # has_planning = collection.get_document(collection="Data", graph_name="count_hasdatum_by_" + period, client=client)
    # has_outlook = collection.get_document(collection="Data", graph_name="count_outlookdatum_by_" + period,
    #                                       client=client) if client == 'kpn' else {}  # temp fix
    # has_voorspeld = collection.get_document(collection="Data", graph_name="count_voorspellingdatum_by_" + period,
    #                                         client=client) if client == 'kpn' else {}  # temp fix

    def _jaaroverzicht(self):
        real, plan = preprocess_for_jaaroverzicht(
            self.intermediate_results.counts_by_month['count_opleverdatum'],
            self.intermediate_results.counts_by_month['count_plandatum'],
        )
        jaaroverzicht = calculate_jaaroverzicht(
            real,
            plan,
            self.intermediate_results.HAS_werkvoorraad,
            self.intermediate_results.HC_HPend
        )
        self.record_dict.add('jaaroverzicht', jaaroverzicht, Record, 'Data')

    def _get_counts_by_month(self):
        logger.info("Calculating counts by month")
        self.intermediate_results.counts_by_month = counts_by_time_period(self.transformed_data.df, freq="MS")
        drl = [dict(record={k: v},
                    graph_name=f"{k}_by_month")
               for k, v in self.intermediate_results.counts_by_month.items()]
        self.record_dict.add('monthly_date_counts', drl, DocumentListRecord, "Data", document_key=['graph_name'])


class TMobileETL(FttXETL, TMobileTransform, TMobileAnalyse):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class TMobileTestETL(PickleExtract, FttXTestLoad, TMobileETL):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
