from google.cloud import firestore
from Analyse.FttX import FttXETL, FttXAnalyse, FttXTransform, PickleExtract, FttXTestLoad, FttXLocalETL
from Analyse.Record.Record import Record
from Analyse.Record.DocumentListRecord import DocumentListRecord
from Analyse.Record.DictRecord import DictRecord
import business_rules as br
from functions import calculate_projectindicators_tmobile, calculate_oplevertijd, wait_bins
from functions_tmobile import calculate_voorraadvormend, add_weeknumber, counts_by_time_period
import logging

logger = logging.getLogger('FttX Analyse')


class TMobileTransform(FttXTransform):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.client_name = kwargs['config'].get('name')

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
        self._make_intermediate_results_tmobile_project_specific_values()
        self._calculate_project_indicators()

    def _get_voorraadvormend(self):
        logger.info("Calculating voorraadvormend")
        record = calculate_voorraadvormend(self.transformed_data.df)
        self.records.add('voorraadvormend', record, Record, "Data")

    # TODO: can this be removed ? Does not seem to be used
    def _get_counts_by_week(self):
        logger.info("Calculating counts by week")
        counts_by_week = counts_by_time_period(self.transformed_data.df)
        drl = [dict(record={k: v},
                    graph_name=f"{k}_by_week")
               for k, v in counts_by_week.items()]
        self.records.add('weekly_date_counts', drl, DocumentListRecord, "Data", document_key=['graph_name'])

    # TODO: can this be removed ? Does not seem to be used
    def _get_counts_by_month(self):
        logger.info("Calculating counts by month")
        self.intermediate_results.counts_by_month = counts_by_time_period(self.transformed_data.df, freq="MS")
        drl = [dict(record={k: v},
                    graph_name=f"{k}_by_month")
               for k, v in self.intermediate_results.counts_by_month.items()]
        self.records.add('monthly_date_counts', drl, DocumentListRecord, "Data", document_key=['graph_name'])

    def _make_intermediate_results_tmobile_project_specific_values(self):
        """
        Calculates the project specific values of openstaande orders for tmobile. These values are extracted as a
        pd.Series with dates, based on the underlying business rules (see br.openstaande_orders_tmobile). The values
        are then calculated per project (obtained from df.groupby('project')) by calculating their lengths OR the
        redenna values of the pd.Series are extracted. The values and redenna values are added into a dictionary
        per project, which is added to intermediate_results.

        Returns: dictionaries with the values per project

        """
        logger.info("Calculating intermediate results for tmobile project specific values")
        df = self.transformed_data.df
        # Create a dictionary that contains the output name and the appropriate masks:
        # Starting with orders that are patch only, followed by HC aanleg:
        function_dict = {
            'openstaand_patch_only_on_time': [
                df[br.openstaande_orders_tmobile(df=df, time_window='on time', order_type='patch only')][
                    ['creation', 'project', 'cluster_redenna']],
                df[br.openstaande_orders_tmobile(df=df, time_window='on time', order_type='patch only', time_delta_days=7)][
                    ['creation', 'project']]
            ],
            'openstaand_patch_only_limited': [
                df[br.openstaande_orders_tmobile(df=df, time_window='limited', order_type='patch only')][
                    ['creation', 'project', 'cluster_redenna']],
                df[br.openstaande_orders_tmobile(df=df, time_window='limited', order_type='patch only', time_delta_days=7)][
                    ['creation', 'project']]
            ],
            'openstaand_patch_only_late': [
                df[br.openstaande_orders_tmobile(df=df, time_window='late', order_type='patch only')][
                    ['creation', 'project', 'cluster_redenna']],
                df[br.openstaande_orders_tmobile(df=df, time_window='late', order_type='patch only', time_delta_days=7)][
                    ['creation', 'project']]
            ],
            'openstaand_hc_aanleg_on_time': [
                df[br.openstaande_orders_tmobile(df=df, time_window='on time', order_type='hc aanleg')][
                    ['creation', 'project', 'cluster_redenna']],
                df[br.openstaande_orders_tmobile(df=df, time_window='on time', order_type='hc aanleg', time_delta_days=7)][
                    ['creation', 'project']]
            ],
            'openstaand_hc_aanleg_limited': [
                df[br.openstaande_orders_tmobile(df=df, time_window='limited', order_type='hc aanleg')][
                    ['creation', 'project', 'cluster_redenna']],
                df[br.openstaande_orders_tmobile(df=df, time_window='limited', order_type='hc aanleg', time_delta_days=7)][
                    ['creation', 'project']]
            ],
            'openstaand_hc_aanleg_late': [
                df[br.openstaande_orders_tmobile(df=df, time_window='late', order_type='hc aanleg')][
                    ['creation', 'project', 'cluster_redenna']],
                df[br.openstaande_orders_tmobile(df=df, time_window='late', order_type='hc aanleg', time_delta_days=7)][
                    ['creation', 'project']]
            ]
        }

        order_time_windows_per_project_dict = {}
        for project, df in df.groupby('project'):
            order_time_windows_dict = {}
            for key, values in function_dict.items():
                value_this_week = len(values[0][values[0].project == project])
                value_last_week = len(values[1][values[1].project == project])
                redenna_this_week = values[0][values[0].project == project].drop(labels=['project'], axis=1)\
                    .groupby(by='cluster_redenna').count()\
                    .rename({'creation': 'cluster_redenna'}, axis=1)\
                    .to_dict()['cluster_redenna']
                # The following dict is made to comply with _calculate_projectindicators_tmobile:
                order_time_windows_dict[key] = {'counts': value_this_week,
                                                'counts_prev': value_last_week,
                                                'cluster_redenna': redenna_this_week}
            order_time_windows_per_project_dict[project] = order_time_windows_dict

        self.intermediate_results.orders_time_windows_per_project = order_time_windows_per_project_dict

    def _calculate_project_indicators(self):
        logger.info("Calculating project indicators")
        counts_by_project = calculate_projectindicators_tmobile(self.transformed_data.df,
                                                                self.intermediate_results.has_werkvoorraad_per_project,
                                                                self.intermediate_results.orders_time_windows_per_project,
                                                                self.intermediate_results.ratio_under_8weeks_per_project)
        self.records.add(key="project_indicators",
                         collection="Data",
                         record_type=DictRecord,
                         record=counts_by_project)

    def _delete_collection(self, collection_name, batch_size=500, count=0):
        logger.info("Deleting collection Houses")
        deleted = 0
        db = firestore.Client()
        batch = db.batch()
        coll_ref = db.collection(collection_name)
        docs = coll_ref.limit(batch_size).stream()
        for doc in docs:
            if doc.exists:
                batch.delete(doc.reference)
                deleted = deleted + 1
            else:
                logging.info(f'{collection_name} does not exists')
                return
        batch.commit()
        logging.info(f'Removing {collection_name}: {count} documents deleted')
        count += batch_size
        if deleted >= batch_size:
            return self._delete_collection(collection_name=collection_name, batch_size=500, count=count)
        else:
            logging.info(f'Removing {collection_name} completed')


class TMobileETL(FttXETL, TMobileTransform, TMobileAnalyse):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class TMobileTestETL(PickleExtract, FttXTestLoad, TMobileETL):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class TMobileLocalETL(FttXLocalETL, TMobileETL):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
