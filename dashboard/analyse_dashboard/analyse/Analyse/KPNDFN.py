import logging

import pandas as pd

from Analyse.FttX import FttXExtract, FttXTransform

logger = logging.getLogger("KPN Analyse")


class KPNDFNExtract(FttXExtract):
    """Extracts the planning excel for HPCiviel and HPend made bij Wout Bisselink from the gcp."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.planning_location = kwargs["config"].get("planning_location")
        self.client_name = kwargs["config"].get("name")

    def extract(self):
        super().extract()
        self._extract_planning()

    def _extract_planning(self):
        logger.info("Extracting Planning")
        if self.planning_location:
            xls = pd.ExcelFile(self.planning_location)
            self.extracted_data.planning = pd.read_excel(xls, "Planning 2021 (2)")
        else:
            self.extracted_data.planning = pd.DataFrame()


class KPNDFNTransform(FttXTransform):
    def transform(self):
        super().transform()
        self._transform_planning_new()

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
            planning_excel.rename(columns={"Unnamed: 1": "project"}, inplace=True)
            df = planning_excel.iloc[:, 20:72].copy()
            df.columns = df.loc[0, :]

            df["project"] = planning_excel["project"].fillna(method="ffill").astype(str)
            df["soort_hp"] = (
                planning_excel.iloc[:, 17]
                .str.lower()
                .str.strip()
                .fillna("hp end")
                .copy()
            )
            df.fillna(0, inplace=True)
            df["project"].replace(
                self.config.get("project_names_planning_map"), inplace=True
            )

            # split hpend and hp_civiel
            df_hpend = df[
                ((df.soort_hp == "hp end") | (df.soort_hp == "status 16"))
            ].copy()
            df_hpciviel = df[df.soort_hp == "hp civiel"].copy()

            # transform the planning into pd.Datafram with index(project, date) and columns(hpend, hpciviel)
            df_hpend_transformed = self._transform_planning_per_kind(
                df=df_hpend, column_name="hp end"
            )
            df_hpciviel_transformed = self._transform_planning_per_kind(
                df=df_hpciviel, column_name="hp civiel"
            )

            # combine hpend and hpciviel and extract totals over all the projects together
            df_transformed_planning = df_hpciviel_transformed.merge(
                df_hpend_transformed, on=["project", "date"], how="outer"
            ).fillna(0)

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
                df_project_transformed = self._transform_planning_per_project(
                    df, project
                )
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
        df_project.columns = ["date", "number"]
        df_project["project"] = project
        df_project = df_project.groupby(["project", "date"]).sum()
        return df_project
