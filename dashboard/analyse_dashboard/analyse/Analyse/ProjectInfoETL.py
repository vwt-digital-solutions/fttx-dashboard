import pandas as pd
import numpy as np
from google.cloud import firestore
from Analyse.ETL import Transform, Load, logger
from Analyse.FttX import FttXExtract
from functions import get_map_bnumber_vs_project_from_sql


class ProjectInfoExtract(FttXExtract):
    """
    Class that extracts project info from the table at the dashboard and the milestone excel file.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.location_of_project_info_excel = kwargs['config'].get("project_info_location")
        self.client_name = self.config.get('name')

    def extract(self):
        logger.info("Extracting the Projects collection")
        self._extract_from_sql()
        logger.info("Extracting mappings of bnumber vs projectname")
        self.extracted_data.map_bnumber_vs_projectname_sql = get_map_bnumber_vs_project_from_sql()
        self.extracted_data.map_bnumber_vs_projectname_fc = self._get_map_bnumber_vs_project_from_fc()
        logger.info("Extracting Project info from firestore as record")
        self.extracted_data.record_project_info = self.get_project_info_record(self.client_name)
        logger.info("Extracting Project info from excel")
        self.extracted_data.excel_project_info = self._get_project_info_excel()

    def _get_map_bnumber_vs_project_from_fc(self):
        ds = self.extracted_data.df[['projectcode', 'project']].set_index('projectcode').drop_duplicates()
        ds.index.name = 'bnumber'
        # exception for bnumber 8258, this value is double assigned to Bergen op Zoom oude stad and Bergen op Zoom oost,
        # so inconclusive and therefore deleted from list
        ds.drop(index='8258', inplace=True)
        return ds

    def _get_project_info_excel(self):
        if self.location_of_project_info_excel:
            xls = pd.ExcelFile(self.location_of_project_info_excel)
        else:
            raise NameError('location of project info excel not declared in config')
        return xls

    def get_project_info_record(self, client):
        record = firestore.Client().collection('ProjectInfo').document(f'{client}_project_dates').get().to_dict()
        return record


class ProjectInfoTransform(Transform):
    """
    Performs necessary transformation steps on extracted project info
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def transform(self, **kwargs):
        """
        Wrapper function to perform all transformation steps for meters.
        """
        logger.info("Making complete mapping of bnumber vs projectname")
        map_bnumber_vs_projectname = self._make_map_bnumber_vs_projectname()
        logger.info("Making project info table based on firestore record")
        df = self._make_table_from_record_project_info()
        logger.info("Making project info table based on excel")
        df_newinfo = self._make_table_from_excel_project_info().drop(columns=['doorlooptijd', 'project'])
        logger.info("Update project info table from record based information in excel")
        df_updated = self.update_project_info(df, df_newinfo, map_bnumber_vs_projectname)
        self.transformed_data.record = df_updated.to_dict(orient='dict')

    def _make_map_bnumber_vs_projectname(self):
        # map of bnumber vs projectname as stored in the sql database
        map_sql = self.extracted_data.map_bnumber_vs_projectname_sql
        # map of bnumber vs projectname as stored in fiberconnect
        map_fc = self.extracted_data.map_bnumber_vs_projectname_fc
        # make map bnumber vs projectname from project info at excel
        map_excel = self._make_table_from_excel_project_info()[['project']]
        # combine information from the three maps into one complete map
        map_complete = map_sql.combine_first(map_excel).combine_first(map_fc)
        map_complete = map_complete.loc[map_complete.index.dropna()]
        return map_complete

    def _make_table_from_excel_project_info(self):
        xls = self.extracted_data.excel_project_info

        if self.client_name == 'kpn':
            name_col_projectname = 'Milestones werkvoorbereidingsplanning KPN Projecten\n\nRevisie: E '
            name_col_projectname += '(HB-schouw voorbereiding/levering eng.)\nstatus: DEFINITIEF - d.d. '
            name_col_projectname += '18-02-2021 - Versie: 0.4'
            df = pd.read_excel(xls, 'KPN')[['KPN B nr.',
                                            'Start Civiel',
                                            '1e BOP',
                                            'aantal KA:',
                                            'BIS (m1)',
                                            'doorlooptijd',
                                            name_col_projectname]]
            df = df.rename(columns={'KPN B nr.': 'bnumber',
                                    'Start Civiel': 'Civiel startdatum',
                                    '1e BOP': 'FTU0',
                                    'aantal KA:': 'huisaansluitingen',
                                    'BIS (m1)': 'meters BIS',
                                    name_col_projectname: 'project'})
        else:
            raise NotImplementedError('This function needs to be extended for tmobile and dfn')

        df = df.set_index('bnumber')

        # cleaning of data
        df = df.loc[df.index.dropna()]
        df.index = [str(i)[1:] if 'B' in str(i) else str(i) for i in df.index]
        df = df.where((df != '???') & (df != '?'))
        # setting required data types
        df['FTU0'] = pd.to_datetime(df['FTU0'])
        df['Civiel startdatum'] = pd.to_datetime(df['Civiel startdatum'])
        # adding required extra columns based on data in other columns
        df['FTU1'] = (df['FTU0'] + pd.to_timedelta(df['doorlooptijd'] * 7, 'd'))
        df['snelheid (m/week)'] = (df['meters BIS'] / df['doorlooptijd'].astype(float)).round()
        df['meters tuinschieten'] = None
        return df

    def _make_table_from_record_project_info(self):
        record = self.extracted_data.record_project_info
        df = pd.DataFrame.from_dict(record['record'], orient='columns')
        # cleaning of data
        df = df.where(df != 'None', None)
        # set required data types
        df['FTU0'] = pd.to_datetime(df['FTU0'])
        df['FTU1'] = pd.to_datetime(df['FTU1'])
        df['Civiel startdatum'] = pd.to_datetime(df['Civiel startdatum'])
        # recalculate snelheid for all projects given meters BIS, FTU0 and FTU1
        df['snelheid (m/week)'] = (df['meters BIS'] / (df['FTU1'] - df['FTU0']).dt.days * 7).round()
        return df

    def update_project_info(self, df, df_newinfo, map_bnumber_vs_projectname):
        for bnumber in df_newinfo.index:
            df.loc[map_bnumber_vs_projectname.loc[bnumber].project] = df_newinfo.loc[bnumber]

        # set required format for use of table at dashboard
        df['Civiel startdatum'] = df['Civiel startdatum'].dt.strftime('%Y-%m-%d')
        df['FTU0'] = df['FTU0'].dt.strftime('%Y-%m-%d')
        df['FTU1'] = df['FTU1'].dt.strftime('%Y-%m-%d')

        # cleaning of data
        df = df.replace({np.nan: None, 'NaT': None}).where(pd.notnull(df), None)
        return df


class ProjectInfoLoad(Load):
    """
    Performs necessary loading steps on transformed project info
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def load(self, **kwargs):
        """
        Wrapper function to perform all loading steps for updated project info.
        """
        logger.info("Loading the updated project info table to the firestore")
        record = self.extracted_data.record_project_info
        record['record'] = self.transformed_data.record
        firestore.Client().collection('ProjectInfo').document(f'{self.client_name}_project_dates').set(record)


# TODO: Documentation by Casper van Houten
class ProjectInfoETL(ProjectInfoExtract, ProjectInfoTransform, ProjectInfoLoad):
    """
    ETL for project info.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def perform(self):
        """
        Performs extract, transform and load
        """
        self.extract()
        self.transform()
        self.load()
