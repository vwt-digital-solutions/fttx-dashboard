from google.cloud import firestore

from Analyse.Data import Data
from Analyse.FttX import FttXExtract, FttXTransform, FttXAnalyse, FttXETL, PickleExtract, FttXTestLoad, FttXLocalETL
from Analyse.Record import ListRecord, IntRecord, StringRecord, Record, DictRecord
from functions import get_data_targets_init, error_check_FCBC, get_start_time, get_timeline, get_total_objects, \
    prognose, targets, performance_matrix, prognose_graph, overview, graph_overview, \
    get_project_dates, \
    analyse_documents, calculate_jaaroverzicht, preprocess_for_jaaroverzicht, calculate_weektarget, calculate_lastweekrealisatie, \
    calculate_weekrealisatie, calculate_bis_gereed, calculate_weekHCHPend, calculate_weeknerr, multi_index_to_dict
import pandas as pd
from Analyse.Timeseries import Timeseries_collection

import logging
from toggles import toggles

logger = logging.getLogger('KPN Analyse')


class KPNDFNExtract(FttXExtract):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.planning_location = kwargs['config'].get("planning_location")
        self.target_location = kwargs['config'].get("target_location")
        self.map_key = kwargs['config'].get('map_key')
        self.client_name = kwargs['config'].get('name')

    def extract(self):
        self._extract_ftu()
        self._extract_planning()
        super().extract()

    def _extract_ftu(self):
        logger.info(f"Extracting FTU {self.client_name}")
        doc = next(
            firestore.Client().collection('Data')
            .where('graph_name', '==', 'project_dates').where('client', '==', self.client_name)
            .stream(), None).get('record')
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


class KPNDFNTransform(FttXTransform):

    def transform(self):
        super().transform()
        self._transform_planning()

    def _transform_planning(self):
        logger.info("Transforming planning for KPN")
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


class KPNAnalyse(FttXAnalyse):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def analyse(self):
        super().analyse()
        logger.info("Analysing using the KPN protocol")
        self._error_check_FCBC()
        self._make_timeseries()
        self._prognose()
        self._targets()
        self._performance_matrix()
        self._prognose_graph()
        self._overview()
        self._calculate_graph_overview()
        self._jaaroverzicht()
        self._calculate_project_indicators()
        self._analysis_documents()
        self._calculate_project_dates()
        self._set_filters()

    def _error_check_FCBC(self):
        logger.info("Calculating errors for KPN")
        n_err, errors_FC_BC = error_check_FCBC(self.transformed_data.df)
        # self.record_dict.add('n_err', n_err, Record, 'Data')
        # self.record_dict.add('errors_FC_BC', errors_FC_BC, Record, 'Data')

        self.intermediate_results.n_err = n_err

    def _make_timeseries(self):
        idx = pd.IndexSlice
        logger.info(f"Generating timeseries for all projects for {self.client_name}")
        opleverdatum_timeseries = Timeseries_collection(self.transformed_data.df,
                                                        column='opleverdatum',
                                                        cutoff=85,
                                                        ftu_dates=self.extracted_data.ftu)

        self.timeseries_frame = opleverdatum_timeseries.get_timeseries_frame()
        self.intermediate_results.d_real_l = multi_index_to_dict(self.timeseries_frame.loc[idx[:], idx[:, 'cumsum_percentage']])
        self.intermediate_results.y_target_l = multi_index_to_dict(self.timeseries_frame.loc[idx[:], idx[:, 'y_target_percentage']])
        self.intermediate_results.y_prog_l = multi_index_to_dict(self.timeseries_frame.loc[idx[:], idx[:, 'prognose_percentage']])

    def _prognose(self):
        logger.info("Calculating prognose for KPN")

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
        self.intermediate_results.d_real_l_old = results.d_real_l
        if not toggles.timeseries:
            self.intermediate_results.d_real_l = results.d_real_l
        self.intermediate_results.x_prog = results.x_prog
        self.intermediate_results.y_prog_l_old = results.y_prog_l
        if not toggles.timeseries:
            self.intermediate_results.y_prog_l = results.y_prog_l
        self.intermediate_results.t_shift = results.t_shift
        self.intermediate_results.cutoff = results.cutoff

        self.record_dict.add('rc1', results.rc1, ListRecord, 'Data')
        self.record_dict.add('rc2', results.rc2, ListRecord, 'Data')
        d_real_l_r = {k: v["Aantal"] for k, v in self.intermediate_results.d_real_l_old.items()}
        self.record_dict.add('d_real_l_r', d_real_l_r, ListRecord, 'Data')
        d_real_l_ri = {k: v.index for k, v in self.intermediate_results.d_real_l_old.items()}
        self.record_dict.add('d_real_l_ri', d_real_l_ri, ListRecord, 'Data')
        self.record_dict.add('y_prog_l', self.intermediate_results.y_prog_l_old, ListRecord, 'Data')
        self.record_dict.add('x_prog', results.x_prog, IntRecord, 'Data')
        self.record_dict.add('t_shift', results.t_shift, StringRecord, 'Data')
        self.record_dict.add('cutoff', results.cutoff, Record, 'Data')

    def _targets(self):
        logger.info("Calculating targets for KPN")
        y_target_l, t_diff = targets(self.intermediate_results.x_prog,
                                     self.intermediate_results.timeline,
                                     self.intermediate_results.t_shift,
                                     self.extracted_data.ftu['date_FTU0'],
                                     self.extracted_data.ftu['date_FTU1'],
                                     self.intermediate_results.rc1,
                                     self.intermediate_results.d_real_l_old)
        self.intermediate_results.y_target_l_old = y_target_l
        if not toggles.timeseries:
            self.intermediate_results.y_target_l = y_target_l
        self.intermediate_results.t_diff = t_diff
        self.record_dict.add('y_target_l', self.intermediate_results.y_target_l_old, ListRecord, 'Data')

    def _performance_matrix(self):
        logger.info("Calculating performance matrix for KPN")
        graph = performance_matrix(
            self.intermediate_results.timeline,
            self.intermediate_results.y_target_l_old,
            self.intermediate_results.d_real_l_old,
            self.intermediate_results.total_objects,
            self.intermediate_results.t_diff,
            self.intermediate_results.y_voorraad_act
        )
        self.record_dict.add('project_performance', graph, Record, 'Graphs')

    def _prognose_graph(self):
        logger.info("Calculating prognose graph for KPN")
        result_dict = prognose_graph(
            self.intermediate_results.timeline,
            self.intermediate_results.y_prog_l,
            self.intermediate_results.d_real_l,
            self.intermediate_results.y_target_l)
        self.record_dict.add('prognose_graph_dict', result_dict, DictRecord, 'Graphs')

    def _overview(self):
        result = overview(self.intermediate_results.timeline,
                          self.intermediate_results.y_prog_l_old,
                          self.intermediate_results.total_objects,
                          self.intermediate_results.d_real_l_old,
                          self.transformed_data.planning,
                          self.intermediate_results.y_target_l_old)
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
            res='W-MON')
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
            res='M')

        self.intermediate_results.data_pr = data_pr
        self.intermediate_results.data_t = data_t
        self.intermediate_results.data_r = data_r
        self.intermediate_results.data_p = data_p

        self.record_dict.add('graph_targets_M', graph_targets_M, Record, 'Graphs')
        self.record_dict.add('count_voorspellingdatum_by_month', data_pr, Record, 'Data')
        self.record_dict.add('count_outlookdatum_by_month', data_t, Record, 'Data')
        self.record_dict.add('count_opleverdatum_by_month', data_r, Record, 'Data')
        self.record_dict.add('count_hasdatum_by_month', data_p, Record, 'Data')

    def _jaaroverzicht(self):
        # Function should not be ran on first pass, as it is called in super constructor.
        # Required variables will not be accessible during call of super constructor.
        if 'df_prog' in self.intermediate_results:
            prog, target, real, plan = preprocess_for_jaaroverzicht(
                self.intermediate_results.df_prog,
                self.intermediate_results.df_target,
                self.intermediate_results.df_real,
                self.intermediate_results.df_plan,
            )

            bis_gereed = calculate_bis_gereed(self.transformed_data.df)
            jaaroverzicht = calculate_jaaroverzicht(
                prog, target, real, plan,
                self.intermediate_results.HAS_werkvoorraad,
                self.intermediate_results.HC_HPend,
                bis_gereed
            )
            self.record_dict.add('jaaroverzicht', jaaroverzicht, Record, 'Data')

    def _calculate_project_indicators(self):
        logger.info("Calculating project indicators")
        projects = self.transformed_data.df.project.unique().to_list()
        record = {}
        for project in projects:
            project_indicators = {}
            weektarget = calculate_weektarget(
                project,
                self.intermediate_results.y_target_l_old,
                self.intermediate_results.total_objects,
                self.intermediate_results.timeline)
            project_df = self.transformed_data.df[self.transformed_data.df.project == project]
            project_indicators['weekrealisatie'] = calculate_weekrealisatie(
                project_df,
                weektarget)
            project_indicators['lastweek_realisatie'] = calculate_lastweekrealisatie(
                project_df,
                weektarget
            )
            project_indicators['weekHCHPend'] = calculate_weekHCHPend(
                project,
                self.intermediate_results.HC_HPend_l)
            project_indicators['weeknerr'] = calculate_weeknerr(
                project,
                self.intermediate_results.n_err)
            record[project] = project_indicators
        graph_name = 'project_indicators'
        self.record_dict.add(graph_name, record, DictRecord, 'Data')

    def _calculate_project_dates(self):
        project_dates = get_project_dates(self.transformed_data.ftu['date_FTU0'],
                                          self.transformed_data.ftu['date_FTU1'],
                                          self.intermediate_results.y_target_l_old,
                                          self.intermediate_results.x_prog,
                                          self.intermediate_results.timeline,
                                          self.intermediate_results.rc1,
                                          self.intermediate_results.d_real_l_old
                                          )
        self.record_dict.add("project_dates", project_dates, Record, "Data")

    def _analysis_documents(self):
        doc2, doc3 = analyse_documents(
            y_target_l=self.intermediate_results.y_target_l_old,
            rc1=self.intermediate_results.rc1,
            x_prog=self.intermediate_results.x_prog,
            x_d=self.intermediate_results.timeline,
            d_real_l=self.intermediate_results.d_real_l_old,
            df_prog=self.intermediate_results.df_prog,
            df_target=self.intermediate_results.df_target,
            df_real=self.intermediate_results.df_real,
            df_plan=self.intermediate_results.df_plan,
            HC_HPend=self.intermediate_results.HC_HPend,
            y_prog_l=self.intermediate_results.y_prog_l_old,
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


class DFNAnalyse(KPNAnalyse):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def analyse(self):
        super().analyse()
        logger.info("Analysing using the KPN protocol")
        self._error_check_FCBC()
        self._prognose()
        self._targets()
        self._performance_matrix()
        self._prognose_graph()
        self._overview()
        self._calculate_graph_overview()
        self._jaaroverzicht()
        self._analysis_documents()
        self._set_filters()


class KPNETL(FttXETL, KPNDFNExtract, KPNDFNTransform, KPNAnalyse):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class KPNTestETL(PickleExtract, FttXTestLoad, KPNETL):
    pass


class KPNLocalETL(FttXLocalETL, KPNETL):
    pass


class DFNETL(FttXETL, KPNDFNExtract, KPNDFNTransform, DFNAnalyse):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class DFNTestETL(PickleExtract, FttXTestLoad, DFNETL):
    pass


class DFNLocalETL(FttXLocalETL, DFNETL):
    pass
