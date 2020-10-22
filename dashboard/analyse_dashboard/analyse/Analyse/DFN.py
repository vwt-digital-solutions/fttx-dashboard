from google.cloud import firestore

from Analyse.Data import Data
from Analyse.FttX import FttXExtract, FttXTransform, FttXAnalyse, FttXETL, PickleExtract, FttXTestLoad, FttXLocalETL
from Analyse.Record import ListRecord, IntRecord, StringRecord, Record, DictRecord
from functions import get_data_targets_init, error_check_FCBC, get_start_time, get_timeline, get_total_objects, \
    prognose, targets, performance_matrix, prognose_graph, overview, graph_overview, \
    analyse_documents, calculate_jaaroverzicht, preprocess_for_jaaroverzicht, get_project_dates
import pandas as pd

import logging

logger = logging.getLogger('DFN Analyse')


class DFNExtract(FttXExtract):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.planning_location = kwargs['config'].get("planning_location")
        self.target_location = kwargs['config'].get("target_location")
        self.map_key = kwargs['config'].get('map_key')

    def extract(self):
        self._extract_ftu()
        self._extract_planning()
        super().extract()

    def _extract_ftu(self):
        logger.info("Extracting FTU")
        doc = next(
            firestore.Client().collection('Data').where('id', '==', 'analysis').where('client', '==', 'dfn').stream(),
            None)
        if doc is not None:
            if doc['FTU0']:
                date_FTU0 = doc['FTU0']
                date_FTU1 = doc['FTU1']
            else:
                logger.warning("FTU0 and FTU1 in firestore are empty, getting from local file")
                date_FTU0, date_FTU1 = get_data_targets_init(self.target_location, self.map_key)
        else:
            logger.warning("Could not retrieve FTU0 and FTU1 from firestore, getting from local file")
            date_FTU0, date_FTU1 = get_data_targets_init(self.target_location, self.map_key)
        self.extracted_data.ftu = Data({'date_FTU0': date_FTU0, 'date_FTU1': date_FTU1})

    def _extract_planning(self):
        logger.info("Extracting Planning")
        if self.planning_location:
            if 'gs://' in self.planning_location:
                xls = pd.ExcelFile(self.planning_location)
            else:
                xls = pd.ExcelFile(self.planning_location)
            df = pd.read_excel(xls, 'FTTX ').fillna(0)
            self.extracted_data.planning = df
        else:
            raise ValueError("No planning_location is configured to extract the planning.")


class DFNTransform(FttXTransform):

    def transform(self):
        super().transform()
        self._transform_planning()

    def _transform_planning(self):
        logger.info("Transforming planning for DFN")
        HP = dict(HPendT=[0] * 52)
        df = self.extracted_data.planning
        for el in df.index:  # Arnhem Presikhaaf toevoegen aan subset??
            if df.loc[el, ('Unnamed: 1')] == 'HP+ Plan':
                HP[df.loc[el, ('Unnamed: 0')]] = df.loc[el][16:68].to_list()
                HP['HPendT'] = [sum(x) for x in zip(HP['HPendT'], HP[df.loc[el, ('Unnamed: 0')]])]
                if df.loc[el, ('Unnamed: 0')] == 'Bergen op Zoom Oude Stad':
                    HP['Bergen op Zoom oude stad'] = HP.pop(df.loc[el, ('Unnamed: 0')])
                if df.loc[el, ('Unnamed: 0')] == 'Arnhem Gulden Bodem':
                    HP['Arnhem Gulden Bodem Schaarsbergen'] = HP.pop(df.loc[el, ('Unnamed: 0')])
                if df.loc[el, ('Unnamed: 0')] == 'Bergen op Zoom Noord':
                    HP['Bergen op Zoom Noord\xa0 wijk 01\xa0+ Halsteren'] = HP.pop(df.loc[el, ('Unnamed: 0')])
                if df.loc[el, ('Unnamed: 0')] == 'Den Haag Bezuidenhout':
                    HP['Den Haag - Haagse Hout-Bezuidenhout West'] = HP.pop(df.loc[el, ('Unnamed: 0')])
                if df.loc[el, ('Unnamed: 0')] == 'Den Haag Morgenstond':
                    HP['Den Haag Morgenstond west'] = HP.pop(df.loc[el, ('Unnamed: 0')])
                if df.loc[el, ('Unnamed: 0')] == 'Den Haag Vrederust Bouwlust':
                    HP['Den Haag - Vrederust en Bouwlust'] = HP.pop(df.loc[el, ('Unnamed: 0')])
                if df.loc[el, ('Unnamed: 0')] == '':
                    HP['KPN Spijkernisse'] = HP.pop(df.loc[el, ('Unnamed: 0')])
                if df.loc[el, ('Unnamed: 0')] == 'Gouda Kort Haarlem':
                    HP['KPN Gouda Kort Haarlem en Noord'] = HP.pop(df.loc[el, ('Unnamed: 0')])
        self.transformed_data.planning = HP


class DFNAnalyse(FttXAnalyse):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def analyse(self):
        super().analyse()
        logger.info("Analysing using the KPN protocol")
        self._error_check_FCBC()
        self._prognose()
        # self._set_input_fields()
        self._targets()
        self._performance_matrix()
        self._prognose_graph()
        self._overview()
        self._calculate_graph_overview()
        self._jaaroverzicht()
        self._analysis_documents()
        self._set_filters()

    def _error_check_FCBC(self):
        logger.info("Calculating errors for DFN")
        n_err, errors_FC_BC = error_check_FCBC(self.transformed_data.df)
        # self.record_dict.add('n_err', n_err, Record, 'Data')
        # self.record_dict.add('errors_FC_BC', errors_FC_BC, Record, 'Data')

        self.intermediate_results.n_err = n_err

    def _prognose(self):
        logger.info("Calculating prognose for DFN")

        start_time = get_start_time(self.transformed_data.df)
        timeline = get_timeline(start_time)
        self.intermediate_results.timeline = timeline

        total_objects = get_total_objects(self.transformed_data.df)
        self.intermediate_results.total_objects = total_objects

        results = prognose(self.transformed_data.df,
                           start_time,
                           timeline,
                           total_objects,
                           self.extracted_data.ftu['date_FTU0'])

        self.intermediate_results.rc1 = results.rc1
        self.intermediate_results.rc2 = results.rc2
        self.intermediate_results.d_real_l = results.d_real_l
        self.intermediate_results.x_prog = results.x_prog
        self.intermediate_results.y_prog_l = results.y_prog_l
        self.intermediate_results.t_shift = results.t_shift
        self.intermediate_results.cutoff = results.cutoff

        self.record_dict.add('rc1', results.rc1, ListRecord, 'Data')
        self.record_dict.add('rc2', results.rc2, ListRecord, 'Data')
        d_real_l_r = {k: v["Aantal"] for k, v in results.d_real_l.items()}
        self.record_dict.add('d_real_l_r', d_real_l_r, ListRecord, 'Data')
        d_real_l_ri = {k: v.index for k, v in results.d_real_l.items()}
        self.record_dict.add('d_real_l_ri', d_real_l_ri, ListRecord, 'Data')
        self.record_dict.add('y_prog_l', results.y_prog_l, ListRecord, 'Data')
        self.record_dict.add('x_prog', results.x_prog, IntRecord, 'Data')
        self.record_dict.add('t_shift', results.t_shift, StringRecord, 'Data')
        self.record_dict.add('cutoff', results.cutoff, Record, 'Data')

    # def _set_input_fields(self):
    #     logger.info("Setting input fields for DFN")
    #     self.record_dict.add("analysis",
    #                          dict(FTU0=self.extracted_data.ftu['date_FTU0'],
    #                               FTU1=self.extracted_data.ftu['date_FTU1']),
    #                          Record,
    #                          "Data")

    #     # TODO is this document still needed? Is the timeline document used instead?
    #     self.record_dict.add("x_d",
    #                          self.intermediate_results.timeline,
    #                          DateRecord,
    #                          collection="Data")

    def _targets(self):
        logger.info("Calculating targets for DFN")
        y_target_l, t_diff = targets(self.intermediate_results.x_prog,
                                     self.intermediate_results.timeline,
                                     self.intermediate_results.t_shift,
                                     self.extracted_data.ftu['date_FTU0'],
                                     self.extracted_data.ftu['date_FTU1'],
                                     self.intermediate_results.rc1,
                                     self.intermediate_results.d_real_l)
        self.intermediate_results.y_target_l = y_target_l
        self.intermediate_results.t_diff = t_diff
        self.record_dict.add('y_target_l', y_target_l, ListRecord, 'Data')

    def _performance_matrix(self):
        logger.info("Calculating performance matrix for DFN")
        graph = performance_matrix(
            self.intermediate_results.timeline,
            self.intermediate_results.y_target_l,
            self.intermediate_results.d_real_l,
            self.intermediate_results.total_objects,
            self.intermediate_results.t_diff,
            self.intermediate_results.y_voorraad_act
        )
        self.record_dict.add('project_performance', graph, Record, 'Graphs')

    def _prognose_graph(self):
        logger.info("Calculating prognose graph for DFN")
        result_dict = prognose_graph(
            self.intermediate_results.timeline,
            self.intermediate_results.y_prog_l,
            self.intermediate_results.d_real_l,
            self.intermediate_results.y_target_l)
        self.record_dict.add('prognose_graph_dict', result_dict, DictRecord, 'Graphs')

    def _overview(self):
        result = overview(self.intermediate_results.timeline,
                          self.intermediate_results.y_prog_l,
                          self.intermediate_results.total_objects,
                          self.intermediate_results.d_real_l,
                          self.transformed_data.planning,
                          self.intermediate_results.y_target_l)
        self.intermediate_results.df_prog = result.df_prog
        self.intermediate_results.df_target = result.df_target
        self.intermediate_results.df_real = result.df_real
        self.intermediate_results.df_plan = result.df_plan

    def _calculate_graph_overview(self):
        logger.info("Calculating graph overview")
        graph_targets_W, data_pr, data_t, data_r, data_p = graph_overview(
            self.intermediate_results.df_prog,
            self.intermediate_results.df_target,
            self.intermediate_results.df_real,
            self.intermediate_results.df_plan,
            self.intermediate_results.HC_HPend,
            self.intermediate_results.HAS_werkvoorraad,
            res='W-MON',
            show_planning=False)
        self.record_dict.add('graph_targets_W', graph_targets_W, Record, 'Graphs')
        self.record_dict.add('count_voorspellingdatum_by_week', data_pr, Record, 'Data')
        self.record_dict.add('count_outlookdatum_by_week', data_t, Record, 'Data')
        self.record_dict.add('count_opleverdatum_by_week', data_r, Record, 'Data')
        self.record_dict.add('count_hasdatum_by_week', data_p, Record, 'Data')

        graph_targets_M, data_pr, data_t, data_r, data_p = graph_overview(
            self.intermediate_results.df_prog,
            self.intermediate_results.df_target,
            self.intermediate_results.df_real,
            self.intermediate_results.df_plan,
            self.intermediate_results.HC_HPend,
            self.intermediate_results.HAS_werkvoorraad,
            res='M',
            show_planning=False)

        self.intermediate_results.data_pr = data_pr
        self.intermediate_results.data_t = data_t
        self.intermediate_results.data_r = data_r
        self.intermediate_results.data_p = data_p

        self.record_dict.add('graph_targets_M', graph_targets_M, Record, 'Graphs')
        self.record_dict.add('count_voorspellingdatum_by_month', data_pr, Record, 'Data')
        self.record_dict.add('count_outlookdatum_by_month', data_t, Record, 'Data')
        self.record_dict.add('count_opleverdatum_by_month', data_r, Record, 'Data')
        self.record_dict.add('count_hasdatum_by_month', data_p, Record, 'Data')

    # has_opgeleverd = collection.get_document(collection="Data", graph_name="count_opleverdatum_by_" + period, client=client)
    # has_planning = collection.get_document(collection="Data", graph_name="count_hasdatum_by_" + period, client=client)
    # has_outlook = collection.get_document(collection="Data", graph_name="count_outlookdatum_by_" + period,
    #                                       client=client) if client == 'kpn' else {}  # temp fix
    # has_voorspeld = collection.get_document(collection="Data", graph_name="count_voorspellingdatum_by_" + period,
    #                                         client=client) if client == 'kpn' else {}  # temp fix

    def _jaaroverzicht(self):
        prog, target, real, plan = preprocess_for_jaaroverzicht(
            self.intermediate_results.df_prog,
            self.intermediate_results.df_target,
            self.intermediate_results.df_real,
            self.intermediate_results.df_plan,
        )
        jaaroverzicht = calculate_jaaroverzicht(
            prog, target, real, plan,
            self.intermediate_results.HAS_werkvoorraad,
            self.intermediate_results.HC_HPend
        )
        self.record_dict.add('jaaroverzicht', jaaroverzicht, Record, 'Data')

    def _calculate_project_dates(self):
        project_dates = get_project_dates(self.transformed_data.ftu['date_FTU0'],
                                          self.transformed_data.ftu['date_FTU1'],
                                          self.intermediate_results.y_target_l,
                                          self.intermediate_results.x_prog,
                                          self.intermediate_results.timeline,
                                          self.intermediate_results.rc1,
                                          self.intermediate_results.d_real_l
                                          )
        self.record_dict.add("project_dates", project_dates, Record, "Data")

    def _analysis_documents(self):
        doc2, doc3 = analyse_documents(
            y_target_l=self.intermediate_results.y_target_l,
            rc1=self.intermediate_results.rc1,
            x_prog=self.intermediate_results.x_prog,
            x_d=self.intermediate_results.timeline,
            d_real_l=self.intermediate_results.d_real_l,
            df_prog=self.intermediate_results.df_prog,
            df_target=self.intermediate_results.df_target,
            df_real=self.intermediate_results.df_real,
            df_plan=self.intermediate_results.df_plan,
            HC_HPend=self.intermediate_results.HC_HPend,
            y_prog_l=self.intermediate_results.y_prog_l,
            tot_l=self.intermediate_results.total_objects,
            HP=self.transformed_data.planning,
            t_shift=self.intermediate_results.t_shift,
            rc2=self.intermediate_results.rc2,
            cutoff=self.intermediate_results.cutoff,
            y_voorraad_act=self.intermediate_results.y_voorraad_act,
            HC_HPend_l=self.intermediate_results.HC_HPend_l,
            Schouw_BIS=self.intermediate_results.Schouw_BIS,
            HPend_l=self.intermediate_results.HPend_l,
            n_err=self.intermediate_results.n_err,
            Schouw=None,
            BIS=None
        )

        self.record_dict.add("analysis2", doc2, Record, "Data")
        self.record_dict.add("analysis3", doc3, Record, "Data")


class DFNETL(FttXETL, DFNExtract, DFNTransform, DFNAnalyse):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class DFNTestETL(PickleExtract, FttXTestLoad, DFNETL):
    pass


class DFNLocalETL(FttXLocalETL, DFNETL):
    pass
