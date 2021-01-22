from google.cloud import firestore

from Analyse.Data import Data
from Analyse.FttX import FttXExtract, FttXTransform, FttXAnalyse, FttXETL, PickleExtract, FttXTestLoad, FttXLocalETL
from Analyse.Record.DictRecord import DictRecord
from Analyse.Record.IntRecord import IntRecord
from Analyse.Record.StringRecord import StringRecord
from Analyse.Record.ListRecord import ListRecord
from Analyse.Record.Record import Record
from functions import error_check_FCBC, get_start_time, get_timeline, get_total_objects, \
    prognose, targets, performance_matrix, prognose_graph, overview, \
    get_project_dates, calculate_weektarget, calculate_lastweek_realisatie_hpend_and_return_graphics, \
    calculate_thisweek_realisatie_hpend_and_return_graphics, make_graphics_for_ratio_hc_hpend_per_project, \
    make_graphics_for_number_errors_fcbc_per_project, calculate_week_target, targets_new
import pandas as pd

import logging
from toggles import toggles

logger = logging.getLogger('KPN Analyse')


class KPNDFNExtract(FttXExtract):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.planning_location = kwargs['config'].get("planning_location")
        self.target_location = kwargs['config'].get("target_location")
        self.client_name = kwargs['config'].get('name')

    def extract(self):
        self._extract_project_info()
        super().extract()

    def _extract_project_info(self):
        logger.info(f"Extracting FTU {self.client_name}")
        doc = firestore.Client().collection('Data')\
            .document(f'{self.client_name}_project_dates')\
            .get().to_dict().get('record')

        self.extracted_data.ftu = Data({'date_FTU0': doc['FTU0'], 'date_FTU1': doc['FTU1']})
        self.extracted_data.civiel_startdatum = doc.get('Civiel startdatum')
        self.extracted_data.total_meters_tuinschieten = doc.get('meters tuinschieten')
        self.extracted_data.total_meters_bis = doc.get('meters BIS')
        self.extracted_data.total_number_huisaansluitingen = doc.get('huisaansluitingen')

        df = pd.DataFrame(doc)
        info_per_project = {}
        for project in df.index:
            info_per_project[project] = df.loc[project].to_dict()
        self.extracted_data.project_info = info_per_project

    # def _extract_planning(self):
    #     logger.info("Extracting Planning")
    #     if self.planning_location:
    #         if 'gs://' in self.planning_location:
    #             xls = pd.ExcelFile(self.planning_location)
    #         else:
    #             xls = pd.ExcelFile(self.planning_location)
    #         df = pd.read_excel(xls, 'FTTX ').fillna(0)
    #         self.extracted_data.planning = df
    #     else:
    #         raise ValueError("No planning_location is configured to extract the planning.")


class KPNDFNTransform(FttXTransform):

    def transform(self):
        super().transform()

    # def _transform_planning(self):
    #     logger.info("Transforming planning for KPN")
    #     HP = dict(HPendT=[0] * 52)
    #     df = self.extracted_data.planning
    #     for el in df.index:  # Arnhem Presikhaaf toevoegen aan subset??
    #         if df.loc[el, ('Unnamed: 1')] == 'HP+ Plan':
    #             HP[df.loc[el, ('Unnamed: 0')]] = df.loc[el][16:68].to_list()
    #             HP['HPendT'] = [sum(x) for x in zip(HP['HPendT'], HP[df.loc[el, ('Unnamed: 0')]])]
    #             if df.loc[el, ('Unnamed: 0')] == 'Bergen op Zoom Oude Stad':
    #                 HP['Bergen op Zoom oude stad'] = HP.pop(df.loc[el, ('Unnamed: 0')])
    #             if df.loc[el, ('Unnamed: 0')] == 'Arnhem Gulden Bodem':
    #                 HP['Arnhem Gulden Bodem Schaarsbergen'] = HP.pop(df.loc[el, ('Unnamed: 0')])
    #             if df.loc[el, ('Unnamed: 0')] == 'Bergen op Zoom Noord':
    #                 HP['Bergen op Zoom Noord\xa0 wijk 01\xa0+ Halsteren'] = HP.pop(df.loc[el, ('Unnamed: 0')])
    #             if df.loc[el, ('Unnamed: 0')] == 'Den Haag Bezuidenhout':
    #                 HP['Den Haag - Haagse Hout-Bezuidenhout West'] = HP.pop(df.loc[el, ('Unnamed: 0')])
    #             if df.loc[el, ('Unnamed: 0')] == 'Den Haag Morgenstond':
    #                 HP['Den Haag Morgenstond west'] = HP.pop(df.loc[el, ('Unnamed: 0')])
    #             if df.loc[el, ('Unnamed: 0')] == 'Den Haag Vrederust Bouwlust':
    #                 HP['Den Haag - Vrederust en Bouwlust'] = HP.pop(df.loc[el, ('Unnamed: 0')])
    #             if df.loc[el, ('Unnamed: 0')] == '':
    #                 HP['KPN Spijkernisse'] = HP.pop(df.loc[el, ('Unnamed: 0')])
    #             if df.loc[el, ('Unnamed: 0')] == 'Gouda Kort Haarlem':
    #                 HP['KPN Gouda Kort Haarlem en Noord'] = HP.pop(df.loc[el, ('Unnamed: 0')])
    #     self.transformed_data.planning = HP


class KPNAnalyse(FttXAnalyse):
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
        if not toggles.new_projectspecific_views:
            self._overview()
        self._calculate_project_indicators()
        self._calculate_project_dates()
        self._set_filters()

    def _error_check_FCBC(self):
        logger.info("Calculating errors for KPN")
        n_err, errors_FC_BC = error_check_FCBC(self.transformed_data.df)
        # self.record_dict.add('n_err', n_err, Record, 'Data')
        # self.record_dict.add('errors_FC_BC', errors_FC_BC, Record, 'Data')

        self.intermediate_results.n_err = n_err

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
        self.intermediate_results.d_real_l = results.d_real_l
        self.intermediate_results.x_prog = results.x_prog
        self.intermediate_results.y_prog_l = results.y_prog_l
        self.intermediate_results.t_shift = results.t_shift
        self.intermediate_results.cutoff = results.cutoff

        self.record_dict.add('rc1', results.rc1, ListRecord, 'Data')
        self.record_dict.add('rc2', results.rc2, ListRecord, 'Data')
        d_real_l_r = {k: v["Aantal"] for k, v in self.intermediate_results.d_real_l.items()}
        self.record_dict.add('d_real_l_r', d_real_l_r, ListRecord, 'Data')
        d_real_l_ri = {k: v.index for k, v in self.intermediate_results.d_real_l.items()}
        self.record_dict.add('d_real_l_ri', d_real_l_ri, ListRecord, 'Data')
        self.record_dict.add('y_prog_l', self.intermediate_results.y_prog_l, ListRecord, 'Data')
        self.record_dict.add('x_prog', results.x_prog, IntRecord, 'Data')
        self.record_dict.add('t_shift', results.t_shift, StringRecord, 'Data')
        self.record_dict.add('cutoff', results.cutoff, Record, 'Data')

    def _targets(self):
        logger.info("Calculating targets for KPN")
        if toggles.new_projectspecific_views:
            y_target_l, t_diff, target_per_week_dict = targets_new(self.intermediate_results.timeline,
                                                                   self.project_list,
                                                                   self.extracted_data.ftu['date_FTU0'],
                                                                   self.extracted_data.ftu['date_FTU1'],
                                                                   self.intermediate_results.total_objects)
            self.intermediate_results.y_target_l = y_target_l
            self.intermediate_results.target_per_week = target_per_week_dict
        else:
            y_target_l, t_diff = targets(self.intermediate_results.x_prog,
                                         self.intermediate_results.timeline,
                                         self.intermediate_results.t_shift,
                                         self.extracted_data.ftu['date_FTU0'],
                                         self.extracted_data.ftu['date_FTU1'],
                                         self.intermediate_results.rc1,
                                         self.intermediate_results.d_real_l,
                                         self.intermediate_results.total_objects)
            self.intermediate_results.y_target_l = y_target_l

        self.intermediate_results.t_diff = t_diff
        self.record_dict.add('y_target_l', self.intermediate_results.y_target_l, ListRecord, 'Data')

    def _performance_matrix(self):
        logger.info("Calculating performance matrix for KPN")
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
        logger.info("Calculating prognose graph for KPN")
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

    # def _calculate_graph_overview(self):
    #     logger.info("Calculating graph overview")
    #     graph_targets_W, data_pr, data_t, data_r, data_p = graph_overview(
    #         self.intermediate_results.df_prog,
    #         self.intermediate_results.df_target,
    #         self.intermediate_results.df_real,
    #         self.intermediate_results.df_plan,
    #         self.intermediate_results.HC_HPend,
    #         self.intermediate_results.HAS_werkvoorraad,
    #         res='W-MON')
    #     self.record_dict.add('graph_targets_W', graph_targets_W, Record, 'Graphs')
    #     self.record_dict.add('count_voorspellingdatum_by_week', data_pr, Record, 'Data')
    #     self.record_dict.add('count_outlookdatum_by_week', data_t, Record, 'Data')
    #     self.record_dict.add('count_opleverdatum_by_week', data_r, Record, 'Data')
    #     self.record_dict.add('count_hasdatum_by_week', data_p, Record, 'Data')
    #
    #     graph_targets_M, data_pr, data_t, data_r, data_p = graph_overview(
    #         self.intermediate_results.df_prog,
    #         self.intermediate_results.df_target,
    #         self.intermediate_results.df_real,
    #         self.intermediate_results.df_plan,
    #         self.intermediate_results.HC_HPend,
    #         self.intermediate_results.HAS_werkvoorraad,
    #         res='M')
    #
    #     self.intermediate_results.data_pr = data_pr
    #     self.intermediate_results.data_t = data_t
    #     self.intermediate_results.data_r = data_r
    #     self.intermediate_results.data_p = data_p
    #
    #     self.record_dict.add('graph_targets_M', graph_targets_M, Record, 'Graphs')
    #     self.record_dict.add('count_voorspellingdatum_by_month', data_pr, Record, 'Data')
    #     self.record_dict.add('count_outlookdatum_by_month', data_t, Record, 'Data')
    #     self.record_dict.add('count_opleverdatum_by_month', data_r, Record, 'Data')
    #     self.record_dict.add('count_hasdatum_by_month', data_p, Record, 'Data')

    # def _jaaroverzicht(self):
    #     # Function should not be ran on first pass, as it is called in super constructor.
    #     # Required variables will not be accessible during call of super constructor.
    #     if 'df_prog' in self.intermediate_results:
    #         prog, target, real, plan = preprocess_for_jaaroverzicht(
    #             self.intermediate_results.df_prog,
    #             self.intermediate_results.df_target,
    #             self.intermediate_results.df_real,
    #             self.intermediate_results.df_plan,
    #         )
    #
    #         bis_gereed = calculate_bis_gereed(self.transformed_data.df)
    #         jaaroverzicht = calculate_jaaroverzicht(
    #             prog, target, real, plan,
    #             self.intermediate_results.HAS_werkvoorraad,
    #             self.intermediate_results.HC_HPend,
    #             bis_gereed
    #         )
    #         self.record_dict.add('jaaroverzicht', jaaroverzicht, Record, 'Data')

    def _calculate_project_indicators(self):
        if toggles.new_projectspecific_views:
            logger.info("Calculating project indicators and making graphic boxes for dashboard")
            df = self.transformed_data.df
            list_of_projects = self.project_list
            record = {}

            for project in list_of_projects:
                project_indicators = {}
                week_target = calculate_week_target(project=project,
                                                    target_per_week=self.intermediate_results.target_per_week,
                                                    FTU0=self.transformed_data.ftu['date_FTU0'],
                                                    FTU1=self.transformed_data.ftu['date_FTU1'],
                                                    time_delta_days=0)
                lastweek_target = calculate_week_target(project=project,
                                                        target_per_week=self.intermediate_results.target_per_week,
                                                        FTU0=self.transformed_data.ftu['date_FTU0'],
                                                        FTU1=self.transformed_data.ftu['date_FTU1'],
                                                        time_delta_days=7)

                project_df = df[df.project == project]

                project_indicators['weekrealisatie'] = calculate_thisweek_realisatie_hpend_and_return_graphics(
                    project_df, week_target)

                project_indicators['lastweek_realisatie'] = calculate_lastweek_realisatie_hpend_and_return_graphics(
                    project_df, lastweek_target)

                project_indicators['weekHCHPend'] = make_graphics_for_ratio_hc_hpend_per_project(
                    project=project, ratio_HC_HPend_per_project=self.intermediate_results.ratio_HC_HPend_per_project)

                project_indicators['weeknerr'] = make_graphics_for_number_errors_fcbc_per_project(
                    project=project, number_errors_per_project=self.intermediate_results.n_err)

                record[project] = project_indicators

            self.record_dict.add('project_indicators', record, DictRecord, 'Data')

        else:
            logger.info("Calculating project indicators")
            df = self.transformed_data.df
            list_of_projects = self.project_list
            record = {}

            for project in list_of_projects:
                project_indicators = {}
                weektarget = calculate_weektarget(project,
                                                  self.intermediate_results.y_target_l,
                                                  self.intermediate_results.total_objects,
                                                  self.intermediate_results.timeline)
                project_df = df[df.project == project]

                project_indicators['weekrealisatie'] = calculate_thisweek_realisatie_hpend_and_return_graphics(
                    project_df, weektarget)

                project_indicators['lastweek_realisatie'] = calculate_lastweek_realisatie_hpend_and_return_graphics(
                    project_df, weektarget)

                project_indicators['weekHCHPend'] = make_graphics_for_ratio_hc_hpend_per_project(
                    project, self.intermediate_results.HC_HPend_l)

                project_indicators['weeknerr'] = make_graphics_for_number_errors_fcbc_per_project(
                    project, self.intermediate_results.n_err)

                record[project] = project_indicators

            self.record_dict.add('project_indicators', record, DictRecord, 'Data')

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

    # def _analysis_documents(self):
    #     doc2, doc3 = analyse_documents(
    #         y_target_l=self.intermediate_results.y_target_l_old,
    #         rc1=self.intermediate_results.rc1,
    #         x_prog=self.intermediate_results.x_prog,
    #         x_d=self.intermediate_results.timeline,
    #         d_real_l=self.intermediate_results.d_real_l_old,
    #         df_prog=self.intermediate_results.df_prog,
    #         df_target=self.intermediate_results.df_target,
    #         df_real=self.intermediate_results.df_real,
    #         df_plan=self.intermediate_results.df_plan,
    #         HC_HPend=self.intermediate_results.HC_HPend,
    #         y_prog_l=self.intermediate_results.y_prog_l_old,
    #         tot_l=self.intermediate_results.total_objects,
    #         HP=self.transformed_data.planning,
    #         t_shift=self.intermediate_results.t_shift,
    #         rc2=self.intermediate_results.rc2,
    #         cutoff=self.intermediate_results.cutoff,
    #         y_voorraad_act=self.intermediate_results.y_voorraad_act,
    #         HC_HPend_l=self.intermediate_results.HC_HPend_l,
    #         Schouw_BIS=self.intermediate_results.Schouw_BIS,
    #         HPend_l=self.intermediate_results.HPend_l,
    #         n_err=self.intermediate_results.n_err,
    #         Schouw=None,
    #         BIS=None
    #     )
    #
    #     self.record_dict.add("analysis2", doc2, Record, "Data")
    #     self.record_dict.add("analysis3", doc3, Record, "Data")


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
