from google.cloud import firestore

from Analyse.Data import Data
from Analyse.FttX import FttXExtract, FttXTransform, FttXAnalyse, FttXETL, PickleExtract, FttXTestLoad, FttXLocalETL
from Analyse.Record.DictRecord import DictRecord
from Analyse.Record.IntRecord import IntRecord
from Analyse.Record.StringRecord import StringRecord
from Analyse.Record.ListRecord import ListRecord
from Analyse.Record.Record import Record
from functions import error_check_FCBC, get_start_time, get_timeline, get_total_objects, \
    lastweek_realisatie_hpend_bullet_chart, \
    thisweek_realisatie_hpend_bullet_chart, \
    prognose, performance_matrix, prognose_graph, \
    make_graphics_for_ratio_hc_hpend_per_project, \
    make_graphics_for_number_errors_fcbc_per_project, calculate_week_target, targets_new
import pandas as pd

import logging

logger = logging.getLogger('KPN Analyse')


class KPNDFNExtract(FttXExtract):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.planning_location = kwargs['config'].get("planning_location")
        self.target_location = kwargs['config'].get("target_location")
        self.client_name = kwargs['config'].get('name')

    def extract(self):
        self._extract_project_info()
        self._extract_planning()
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

    def _extract_planning(self):
        logger.info("Extracting Planning")
        if self.planning_location:
            xls = pd.ExcelFile(self.planning_location)
            self.extracted_data.planning = pd.read_excel(xls, 'Planning 2021 (2)')
        else:
            self.extracted_data.planning = pd.DataFrame()


class KPNDFNTransform(FttXTransform):

    def transform(self):
        super().transform()
        self._transform_planning()

    def _transform_planning(self):
        logger.info("Transforming planning for KPN")
        planning_excel = self.extracted_data.get("planning", pd.DataFrame())
        if not planning_excel.empty:
            planning_excel.rename(columns={'Unnamed: 1': 'project'}, inplace=True)
            df = planning_excel.iloc[:, 20:72].copy()
            df['project'] = planning_excel['project'].astype(str)
            df.fillna(0, inplace=True)
            df['project'].replace(self.config.get('project_names_planning_map'), inplace=True)

            empty_list = [0] * 52
            hp = {}
            hp_end_t = empty_list
            for project in self.project_list:
                if project in df.project.unique():
                    weeks_list = list(df[df.project == project].iloc[0][0:-1])
                    hp[project] = weeks_list
                    hp_end_t = [x + y for x, y in zip(hp_end_t, weeks_list)]
                else:
                    hp[project] = empty_list
                    hp_end_t = [x + y for x, y in zip(hp_end_t, empty_list)]
            hp['HPendT'] = hp_end_t
        else:
            hp = {}
        self.transformed_data.planning = hp


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
        self._calculate_project_indicators()
        self._set_filters()

    def _error_check_FCBC(self):
        logger.info("Calculating errors for KPN")
        n_errors_FCBC, errors_FC_BC = error_check_FCBC(self.transformed_data.df)
        # self.record_dict.add('n_err', n_err, Record, 'Data')
        # self.record_dict.add('errors_FC_BC', errors_FC_BC, Record, 'Data')
        self.intermediate_results.n_errors_FCBC = n_errors_FCBC

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

        self.records.add('rc1', results.rc1, ListRecord, 'Data')
        self.records.add('rc2', results.rc2, ListRecord, 'Data')
        d_real_l_r = {k: v["Aantal"] for k, v in self.intermediate_results.d_real_l.items()}
        self.records.add('d_real_l_r', d_real_l_r, ListRecord, 'Data')
        d_real_l_ri = {k: v.index for k, v in self.intermediate_results.d_real_l.items()}
        self.records.add('d_real_l_ri', d_real_l_ri, ListRecord, 'Data')
        self.records.add('y_prog_l', self.intermediate_results.y_prog_l, ListRecord, 'Data')
        self.records.add('x_prog', results.x_prog, IntRecord, 'Data')
        self.records.add('t_shift', results.t_shift, StringRecord, 'Data')
        self.records.add('cutoff', results.cutoff, Record, 'Data')

    def _targets(self):
        logger.info("Calculating targets for KPN")
        y_target_l, t_diff, target_per_week_dict = targets_new(self.intermediate_results.timeline,
                                                               self.project_list,
                                                               self.extracted_data.ftu['date_FTU0'],
                                                               self.extracted_data.ftu['date_FTU1'],
                                                               self.intermediate_results.total_objects)
        self.intermediate_results.y_target_l = y_target_l
        self.intermediate_results.target_per_week = target_per_week_dict

        self.intermediate_results.t_diff = t_diff
        self.records.add('y_target_l', self.intermediate_results.y_target_l, ListRecord, 'Data')

    def _performance_matrix(self):
        logger.info("Calculating performance matrix for KPN")
        graph = performance_matrix(
            self.intermediate_results.timeline,
            self.intermediate_results.y_target_l,
            self.intermediate_results.d_real_l,
            self.intermediate_results.total_objects,
            self.intermediate_results.t_diff,
            self.intermediate_results.current_werkvoorraad
        )
        self.records.add('project_performance', graph, Record, 'Graphs')

    def _prognose_graph(self):
        logger.info("Calculating prognose graph for KPN")
        result_dict = prognose_graph(
            self.intermediate_results.timeline,
            self.intermediate_results.y_prog_l,
            self.intermediate_results.d_real_l,
            self.intermediate_results.y_target_l,
            self.extracted_data.ftu['date_FTU0'],
            self.extracted_data.ftu['date_FTU1']
        )
        self.records.add('prognose_graph_dict', result_dict, DictRecord, 'Graphs')

    def _calculate_project_indicators(self):
        logger.info("Calculating project indicators and making graphic boxes for dashboard")
        record = {}
        for project in self.project_list:
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

            project_df = self.transformed_data.df[self.transformed_data.df.project == project]

            project_indicators['weekrealisatie'] = thisweek_realisatie_hpend_bullet_chart(
                project_df, week_target)

            project_indicators['lastweek_realisatie'] = lastweek_realisatie_hpend_bullet_chart(
                project_df, lastweek_target)

            project_indicators['weekHCHPend'] = make_graphics_for_ratio_hc_hpend_per_project(
                project=project, ratio_HC_HPend_per_project=self.intermediate_results.ratio_HC_HPend_per_project)

            project_indicators['weeknerr'] = make_graphics_for_number_errors_fcbc_per_project(
                project=project, number_errors_per_project=self.intermediate_results.n_errors_FCBC)

            record[project] = project_indicators

        self.records.add('project_indicators', record, DictRecord, 'Data')


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
        self._set_filters()
        self._calculate_project_indicators()


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
