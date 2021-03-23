from Analyse.FttX import FttXExtract, FttXTransform, FttXAnalyse, FttXETL, PickleExtract, FttXTestLoad, FttXLocalETL
from Analyse.Record.DictRecord import DictRecord
from Analyse.Record.IntRecord import IntRecord
from Analyse.Record.StringRecord import StringRecord
from Analyse.Record.ListRecord import ListRecord
from Analyse.Record.Record import Record
import business_rules as br
from functions import error_check_FCBC, get_start_time, get_timeline, get_total_objects, \
    lastweek_realisatie_hpend_bullet_chart, \
    thisweek_realisatie_hpend_bullet_chart, \
    prognose, performance_matrix, prognose_graph, \
    make_graphics_for_ratio_hc_hpend_per_project, \
    make_graphics_for_number_errors_fcbc_per_project, calculate_week_target, targets_new, \
    extract_bis_target_project, week_realisatie_bullet_chart
import pandas as pd
import datetime

import logging

logger = logging.getLogger('KPN Analyse')


# TODO: Documentation by Andre van Turnhout
class KPNDFNExtract(FttXExtract):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.planning_location = kwargs['config'].get("planning_location")
        self.target_location = kwargs['config'].get("target_location")
        self.client_name = kwargs['config'].get('name')

    # TODO: Documentation by Andre van Turnhout
    def extract(self):
        super().extract()
        self._extract_planning()

    # TODO: Documentation by Andre van Turnhout
    def _extract_planning(self):
        logger.info("Extracting Planning")
        if self.planning_location:
            xls = pd.ExcelFile(self.planning_location)
            self.extracted_data.planning = pd.read_excel(xls, 'Planning 2021 (2)')
        else:
            self.extracted_data.planning = pd.DataFrame()


# TODO: Documentation by Andre van Turnhout
class KPNDFNTransform(FttXTransform):

    # TODO: Documentation by Andre van Turnhout
    def transform(self):
        super().transform()
        self._transform_planning()
        self._transform_planning_new()

    # TODO: Documentation by Andre van Turnhout
    def _transform_planning(self):
        logger.info("Transforming planning for KPN")
        planning_excel = self.extracted_data.get("planning", pd.DataFrame())
        if not planning_excel.empty:
            planning_excel.rename(columns={'Unnamed: 1': 'project'}, inplace=True)
            df = planning_excel.iloc[:, 20:72].copy()
            df['project'] = planning_excel['project'].fillna(method='ffill').astype(str)
            df['soort_hp'] = planning_excel.iloc[:, 17].replace('HP end', 'Hp End').fillna('Hp End').copy()
            df.fillna(0, inplace=True)
            df = df[((df.soort_hp == 'Hp End') | (df.soort_hp == 'Status 16'))].copy()
            df['project'].replace(self.config.get('project_names_planning_map'), inplace=True)

            empty_list = [0] * 52
            hp = {}
            hp_end_total = empty_list
            for project in self.project_list:
                if project in df.project.unique():
                    weeks_list = list(df[df.project == project].iloc[0][0:-1])
                    hp[project] = weeks_list
                    hp_end_total = [x + y for x, y in zip(hp_end_total, weeks_list)]
                else:
                    hp[project] = empty_list
                    hp_end_total = [x + y for x, y in zip(hp_end_total, empty_list)]
            hp['HPendT'] = hp_end_total
        else:
            hp = {}
        self.transformed_data.planning = hp

    def _transform_planning_new(self):
        """
        This function extracts the planned number of HPend and HPCiviel / per project / per week from a excel. This
        Excel file is used by the projectleaders and updated monthly.

        The extracted planning is in the form of a pd.DataFrame with index=[project, date] and columns=[hpend, hpciviel]
        """
        logging.info("Transforming planning for KPN")
        planning_excel = self.extracted_data.get("planning", pd.DataFrame())
        if not planning_excel.empty:
            # Extract the right data from the excel
            planning_excel.rename(columns={'Unnamed: 1': 'project'}, inplace=True)
            df = planning_excel.iloc[:, 20:72].copy()
            df.columns = df.loc[0, :]

            df['project'] = planning_excel['project'].fillna(method='ffill').astype(str)
            df['soort_hp'] = planning_excel.iloc[:, 17].str.lower().str.strip().fillna('hp end').copy()
            df.fillna(0, inplace=True)
            df['project'].replace(self.config.get('project_names_planning_map'), inplace=True)

            # split hpend and hp_civiel
            df_hpend = df[((df.soort_hp == 'hp end') | (df.soort_hp == 'status 16'))].copy()
            df_hpciviel = df[df.soort_hp == 'hp civiel'].copy()

            # transform the planning into pd.Datafram with index(project, date) and columns(hpend, hpciviel)
            df_hpend_transformed = self._transform_planning_per_kind(df=df_hpend,
                                                                     column_name='hp end')
            df_hpciviel_transformed = self._transform_planning_per_kind(df=df_hpciviel,
                                                                        column_name='hp civiel')

            # combine hpend and hpciviel and extract totals over all the projects together
            df_transformed_planning = df_hpciviel_transformed.merge(df_hpend_transformed,
                                                                    on=['project', 'date'],
                                                                    how='outer').fillna(0)

            self.transformed_data.planning_new = df_transformed_planning

    def _transform_planning_per_kind(self, df, column_name):
        """
        This functions transforms a df into the right format. The input is the dataframe which holds the info on
        hpend or on hpciviel. The format returned is a dataframe with double index=[project, date] and
        column=hpend or hpciviel

        Args:
            df: pd.DataFrame: dataframe with the hpend or hpciviel data
            column_name: str of column name to output

        Returns: pd.DataFrame: The planning of all the projects in a dataframe
                               with index=[project, date] and column=planning

        """
        df_transformed = pd.DataFrame()
        for project in self.projects:
            if project in df.project.unique():
                df_project_transformed = self._transform_planning_per_project(df, project)
                df_transformed = df_transformed.append(df_project_transformed)
        df_transformed.columns = [column_name]
        return df_transformed

    def _transform_planning_per_project(self, df, project):
        """

        Args:
            df: pd.DataFrame: a dataframe containing the planning for a specific project
            project: project that exists in the dataframe

        Returns: pd.DataFrame: The planning of a project in a dataframe with index=[project, data] and column=planning

        """
        df_project = df[df.project == project].iloc[0][0:-2].copy().reset_index()
        df_project.columns = ['date', 'number']
        df_project['project'] = project
        df_project = df_project.groupby(['project', 'date']).sum()
        return df_project


# TODO: Documentation by Andre van Turnhout
class KPNAnalyse(FttXAnalyse):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    # TODO: Documentation by Andre van Turnhout
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

    # TODO: Documentation by Andre van Turnhout
    def _error_check_FCBC(self):
        logger.info("Calculating errors for KPN")
        n_errors_FCBC, errors_FC_BC = error_check_FCBC(self.transformed_data.df)
        # self.record_dict.add('n_err', n_err, Record, 'Data')
        # self.record_dict.add('errors_FC_BC', errors_FC_BC, Record, 'Data')
        self.intermediate_results.n_errors_FCBC = n_errors_FCBC

    # TODO: Documentation by Casper van Houten
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
                           self.transformed_data.ftu['date_FTU0'])

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
        # self.records.add('y_prog_l', self.intermediate_results.y_prog_l, ListRecord, 'Data')
        self.records.add('x_prog', results.x_prog, IntRecord, 'Data')
        self.records.add('t_shift', results.t_shift, StringRecord, 'Data')
        self.records.add('cutoff', results.cutoff, Record, 'Data')

    # TODO: Documentation by Tjeerd Pols
    def _targets(self):
        logger.info("Calculating targets for KPN")
        y_target_l, t_diff, target_per_week_dict = targets_new(self.intermediate_results.timeline,
                                                               self.project_list,
                                                               self.transformed_data.ftu['date_FTU0'],
                                                               self.transformed_data.ftu['date_FTU1'],
                                                               self.intermediate_results.total_objects)
        self.intermediate_results.y_target_l = y_target_l
        self.intermediate_results.target_per_week = target_per_week_dict

        self.intermediate_results.t_diff = t_diff
        # self.records.add('y_target_l', self.intermediate_results.y_target_l, ListRecord, 'Data')

    # TODO: Documentation by Andre van Turnhout
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

    # TODO: Documentation by Tjeerd Pols
    def _prognose_graph(self):
        logger.info("Calculating prognose graph for KPN")
        result_dict = prognose_graph(
            self.intermediate_results.timeline,
            self.intermediate_results.y_prog_l,
            self.intermediate_results.d_real_l,
            self.intermediate_results.y_target_l,
            self.transformed_data.ftu['date_FTU0'],
            self.transformed_data.ftu['date_FTU1']
        )
        self.records.add('prognose_graph_dict', result_dict, DictRecord, 'Graphs')

    # TODO: Documentation by Tjeerd Pols
    def _calculate_project_indicators(self):
        logger.info("Calculating project indicators and making graphic boxes for dashboard")
        today = datetime.datetime.today()
        day_of_week = today.weekday()
        yesterday = today - datetime.timedelta(1)
        next_sunday = today + datetime.timedelta(6 - day_of_week)
        sunday_last_week = today - datetime.timedelta(day_of_week + 1)
        sunday_two_weeks = sunday_last_week - datetime.timedelta(7)
        record = {}
        for project in self.project_list:
            project_indicators = {}

            # hpend targets this week and last week
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

            # bis realisatie and target dates
            bis_realisatie_dates = project_df[br.bis_opgeleverd(project_df)].status_civiel_datum
            bis_realisatie_dates = bis_realisatie_dates.value_counts().resample('D').sum()
            bis_target_dates = extract_bis_target_project(
                civiel_startdatum=self.transformed_data.get('civiel_startdatum').get(project),
                total_meters_bis=self.transformed_data.get('total_meters_bis').get(project),
                total_num_has=self.transformed_data.get('total_number_huisaansluitingen').get(project),
                snelheid_m_week=self.transformed_data.get('snelheid_mpw').get(project))

            # bis realisaties
            this_week_bis_realisatie = int(bis_realisatie_dates[sunday_last_week:today].sum())
            this_week_bis_realisatie_yesterday = int(bis_realisatie_dates[sunday_last_week:yesterday].sum())
            last_week_bis_realisatie = int(bis_realisatie_dates[sunday_two_weeks:sunday_last_week].sum())

            # bis targets
            this_week_bis_target = int(bis_target_dates[sunday_last_week:next_sunday].sum())
            last_week_bis_target = int(bis_target_dates[sunday_two_weeks:sunday_last_week].sum())

            # graphics
            project_indicators['weekrealisatie'] = thisweek_realisatie_hpend_bullet_chart(
                project_df, week_target)

            project_indicators['lastweek_realisatie'] = lastweek_realisatie_hpend_bullet_chart(
                project_df, lastweek_target)

            project_indicators['week_bis_realisatie'] = week_realisatie_bullet_chart(this_week_bis_realisatie,
                                                                                     this_week_bis_realisatie_yesterday,
                                                                                     this_week_bis_target)

            project_indicators['last_week_bis_realisatie'] = week_realisatie_bullet_chart(last_week_bis_realisatie,
                                                                                          None,
                                                                                          last_week_bis_target,
                                                                                          week_delta=1)

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
