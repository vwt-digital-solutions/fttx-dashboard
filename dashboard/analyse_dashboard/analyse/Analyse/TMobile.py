from Analyse.FttX import FttXETL, FttXAnalyse, FttXTransform, PickleExtract, FttXTestLoad
from Analyse.Record import Record, DocumentListRecord, DictRecord
import business_rules as br
from functions import calculate_projectindicators_tmobile, wait_bins
from functions_tmobile import calculate_voorraadvormend, add_weeknumber, preprocess_for_jaaroverzicht
from functions_tmobile import counts_by_time_period, calculate_jaaroverzicht
from functions import calculate_on_time_ratio, calculate_oplevertijd
import logging
logger = logging.getLogger('T-mobile Analyse')


class TMobileTransform(FttXTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def transform(self, **kwargs):
        super().transform(**kwargs)
        self._HAS_add_weeknumber()
        self._georderd()
        self._opgeleverd()
        self._calculate_oplevertijd()
        self._waiting_category()

    def _georderd(self):
        # Iedere woning met een toestemmingsdatum is geordered door T-mobile.
        self.transformed_data.df['ordered'] = br.ordered(self.transformed_data.df)

    def _opgeleverd(self):
        # Iedere woning met een opleverdatum is opgeleverd.
        self.transformed_data.df['opgeleverd'] = br.opgeleverd(self.transformed_data.df)

    def _calculate_oplevertijd(self):
        # Oplevertijd is het verschil tussen de toestemmingsdatum en opleverdatum, in dagen.
        self.transformed_data.df['oplevertijd'] = self.transformed_data.df.apply(lambda x: calculate_oplevertijd(x), axis='columns')

    def _HAS_add_weeknumber(self):
        self.transformed_data.df['has_week'] = add_weeknumber(self.transformed_data.df['hasdatum'])

    def _waiting_category(self):
        toestemming_df = wait_bins(self.transformed_data.df)
        toestemming_df_prev = wait_bins(self.transformed_data.df, time_delta_days=7)
        self.transformed_data.df['wait_category'] = toestemming_df.bins
        self.transformed_data.df['wait_category_minus_delta'] = toestemming_df_prev.bins


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
        self._calculate_project_indicators()
        self._endriched_data()

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

    def _jaaroverzicht(self):
        # Function should not be ran on first pass, as it is called in super constructor.
        # Required variables will not be accessible during call of super constructor.
        if 'counts_by_month' in self.intermediate_results:
            real, plan = preprocess_for_jaaroverzicht(
                self.intermediate_results.counts_by_month['count_opleverdatum'],
                self.intermediate_results.counts_by_month['count_hasdatum'],
            )
            on_time_ratio = calculate_on_time_ratio(self.transformed_data.df)
            outlook = self.transformed_data.df['ordered'].sum()
            jaaroverzicht = calculate_jaaroverzicht(
                real,
                plan,
                self.intermediate_results.HAS_werkvoorraad,
                self.intermediate_results.HC_HPend,
                on_time_ratio,
                outlook
            )
            self.record_dict.add('jaaroverzicht', jaaroverzicht, Record, 'Data')

    def _get_counts_by_month(self):
        logger.info("Calculating counts by month")
        self.intermediate_results.counts_by_month = counts_by_time_period(self.transformed_data.df, freq="MS")
        drl = [dict(record={k: v},
                    graph_name=f"{k}_by_month")
               for k, v in self.intermediate_results.counts_by_month.items()]
        self.record_dict.add('monthly_date_counts', drl, DocumentListRecord, "Data", document_key=['graph_name'])

    def _calculate_project_indicators(self):
        logger.info("Calculating project indicators")
        counts_by_project = calculate_projectindicators_tmobile(self.transformed_data.df)
        self.record_dict.add(key="project_indicators",
                             collection="Data",
                             RecordType=DictRecord,
                             record=counts_by_project)

    def _endriched_data(self):
        df_copy = self.transformed_data.df.copy()
        datums = [col for col in df_copy.columns if "datum" in col]
        df_copy.loc[:, datums] = df_copy[datums].apply(lambda x: x.dt.strftime("%Y-%m-%d"))
        doc_list = [{'record': x, 'sleutel': x['sleutel']} for x in df_copy.to_dict(orient='rows')]
        self.record_dict.add('enriched_data', doc_list, DocumentListRecord, 'Houses', document_key=['sleutel'])


class TMobileETL(FttXETL, TMobileTransform, TMobileAnalyse):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class TMobileTestETL(PickleExtract, FttXTestLoad, TMobileETL):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class TMobileLocalETL(PickleExtract, TMobileETL):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
