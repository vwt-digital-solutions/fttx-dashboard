import pandas as pd
import numpy as np
from google.cloud import firestore
from datetime import timedelta
from Analyse.ETL import ETL, Transform, Load, logger
from Analyse.FttX import FttXExtract
from functions import get_bnumber_project_mapping


class ProjectInfoExtract(FttXExtract):
    """
    Class that extracts project info from the table at the dashboard and the milestone excel file.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.project_info_location = kwargs['config'].get("project_info_location")
        self.client_name = self.config.get('name')

    def extract(self):
        if self.project_info_location:
            logger.info("Extracting the Projects collection")
            self._extract_from_sql()
            logger.info("Extracting Project info")
            self.extracted_data.bnummer_to_projectname = self.get_complete_map_bnummer_to_projectname()
            self.extracted_data.record, self.extracted_data.df_table_now = self.get_df_table_now()
            self.extracted_data.df_table_excel = self.get_df_table_excel()
        else:
            raise NameError('No path to project info excel is specified')

    def get_complete_map_bnummer_to_projectname(self):
        # get mapping from table in sql database
        mapping = get_bnumber_project_mapping()
        bnummer_to_projectname_sql = pd.DataFrame(data=mapping.values(),
                                                  index=mapping.keys()).rename(columns={0: 'project'})
        bnummer_to_projectname_sql.index.name = 'bnummer'

        # get mapping from fiberconnect
        ds = self.extracted_data.df[['projectcode', 'project']].rename(columns={'projectcode': 'bnummer'})
        bnummer_to_projectname_fc = ds[~ds.duplicated()].set_index('bnummer')
        bnummer_to_projectname_fc.drop(index='8258', inplace=True)

        # get mapping from project info excel sheet
        xls = pd.ExcelFile(self.project_info_location)
        if self.client_name == 'kpn':
            test_project_info = pd.read_excel(xls, 'KPN')
            name_col_projectname = 'Milestones werkvoorbereidingsplanning KPN Projecten\n\nRevisie: E '
            name_col_projectname += '(HB-schouw voorbereiding/levering eng.)\nstatus: DEFINITIEF - d.d. '
            name_col_projectname += '18-02-2021 - Versie: 0.4'
            ds = test_project_info[['KPN B nr.', name_col_projectname]]
            ds = ds.rename(columns={'KPN B nr.': 'bnummer', name_col_projectname: 'project'})
            ds = ds[~ds['bnummer'].isna()].set_index('bnummer')
            ds.index = [str(i)[1:] if 'B' in str(i) else i for i in ds.index]
            ds.index.name = 'bnummer'
            bnummer_to_projectname_ex = ds
        else:
            raise NotImplementedError('This function needs to be extended for tmobile and dfn')

        # combine information from the three maps
        bnummer_to_projectname = bnummer_to_projectname_sql.combine_first(bnummer_to_projectname_ex).combine_first(
            bnummer_to_projectname_fc)
        bnummer_to_projectname = bnummer_to_projectname[~bnummer_to_projectname.index.isna()].to_dict()['project']

        return bnummer_to_projectname

    def get_df_table_now(self):
        record = firestore.Client().collection('ProjectInfo').document('kpn_project_dates').get().to_dict()
        df_table = pd.DataFrame.from_dict(record['record'], orient='columns').replace({'None': None})
        df_table['FTU0'] = [pd.to_datetime(el) for el in df_table['FTU0']]
        df_table['FTU1'] = [pd.to_datetime(el) for el in df_table['FTU1']]
        df_table['Civiel startdatum'] = [pd.to_datetime(el) for el in df_table['Civiel startdatum']]

        return record, df_table

    def get_df_table_excel(self):
        xls = pd.ExcelFile(self.project_info_location)
        if self.client_name == 'kpn':
            test_project_info = pd.read_excel(xls, 'KPN')
            df_table = test_project_info[['KPN B nr.',
                                          'Start Civiel',
                                          '1e BOP', 'aantal KA:',
                                          'BIS (m1)', 'doorlooptijd']]
            df_table = df_table[~df_table['KPN B nr.'].isna()].set_index('KPN B nr.')
            df_table.index = [str(i)[1:] if 'B' in str(i) else i for i in df_table.index]
            df_table.rename(columns={'Start Civiel': 'Civiel startdatum',
                                     '1e BOP': 'FTU0',
                                     'aantal KA:': 'huisaansluitingen',
                                     'BIS (m1)': 'meters BIS'},
                            inplace=True)
            df_table = df_table.replace({'???': None, '?': None}).fillna(999).replace({999: None})
            df_table['FTU0'] = [pd.to_datetime(el) for el in df_table['FTU0']]
            df_table['Civiel startdatum'] = [pd.to_datetime(el) for el in df_table['Civiel startdatum']]
            df_table['FTU1'] = [FTU0 + timedelta(w*7) if (FTU0 is not None) & (w is not None) else None
                                for FTU0, w in zip(df_table['FTU0'], df_table['doorlooptijd'])]
            df_table['snelheid (m/week)'] = [int(mBIS / w) if (mBIS is not None) & (w is not None) else None
                                             for mBIS, w in zip(df_table['meters BIS'], df_table['doorlooptijd'])]
            df_table['meters tuinschieten'] = None
            df_table = df_table.drop(columns=['doorlooptijd'])
        else:
            raise NotImplementedError('This function needs to be extended for tmobile and dfn')

        return df_table


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
        super().transform()
        logger.info("Transforming the data to create workable pd DataFrame")
        self._transform_project_info()

    def _transform_project_info(self):
        bnummer_to_projectname = self.extracted_data.bnummer_to_projectname
        df_table_now = self.extracted_data.df_table_now
        df_table_excel = self.extracted_data.df_table_excel
        record = self.extracted_data.record

        for bnummer in bnummer_to_projectname:
            # project info in table is updated with newest information from excel
            if (bnummer_to_projectname[bnummer] in df_table_now.index) & (bnummer in df_table_excel.index):
                df_table_now.loc[bnummer_to_projectname[bnummer]] = df_table_excel.loc[bnummer]
            # table is extended with new project from excel
            elif (bnummer_to_projectname[bnummer] not in df_table_now.index) & (bnummer in df_table_excel.index):
                df_table_now.loc[bnummer_to_projectname[bnummer]] = df_table_excel.loc[bnummer]

        df_table_now['snelheid (m/week)'] = df_table_now['meters BIS'] /\
            (df_table_now['FTU1'] - df_table_now['FTU0']).dt.days * 7
        df_table_now.loc[~df_table_now['snelheid (m/week)'].isna(), 'snelheid (m/week)'] =\
            df_table_now[~df_table_now['snelheid (m/week)'].isna()]['snelheid (m/week)'].astype(int)

        df_table_now = df_table_now.replace({np.nan: 999, pd.NaT: 999}).replace({999: None})
        df_table_now['Civiel startdatum'] = [el.strftime('%Y-%m-%d') if el is not None else el
                                             for el in df_table_now['Civiel startdatum']]
        df_table_now['FTU0'] = [el.strftime('%Y-%m-%d') if el is not None else el for el in df_table_now['FTU0']]
        df_table_now['FTU1'] = [el.strftime('%Y-%m-%d') if el is not None else el for el in df_table_now['FTU1']]

        record['record'] = df_table_now.to_dict(orient='dict')
        self.transformed_data.record = record


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
        super().load()
        logger.info("Loading the data to firestore")
        firestore.Client().collection('ProjectInfo').document(self.client_name + '_project_dates').set(
            self.transformed_data.record)


# TODO: Documentation by Casper van Houten
class ProjectInfoETL(ETL, ProjectInfoExtract, ProjectInfoTransform, ProjectInfoLoad):
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
