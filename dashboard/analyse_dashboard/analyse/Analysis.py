try:
    from functions import prognose, targets, error_check_FCBC, calculate_projectspecs, graph_overview, \
        performance_matrix, \
        calculate_y_voorraad_act, prognose_graph, info_table, overview_reden_na, individual_reden_na, set_filters
    from Record import Record, ListRecord, StringRecord, DateRecord, IntRecord, DictRecord
except ImportError:
    from analyse.functions import prognose, targets, error_check_FCBC, calculate_projectspecs, graph_overview, \
        performance_matrix, \
        calculate_y_voorraad_act, prognose_graph, info_table, overview_reden_na, individual_reden_na, set_filters
    from analyse.Record import Record, ListRecord, StringRecord, DateRecord, IntRecord, DictRecord


class Analysis:

    def __init__(self, client):
        self._client = client

    def __repr__(self):

        return f"Analysis(client={self._client})"

    def __str__(self):

        fields = [field_name for field_name, data in self.__dict__.items() if not field_name[0] == "_"]

        return f"Analysis(client={self._client}) containing: {fields}"

    def _repr_html_(self):
        rows = "\n".join(
            [data.to_table_part(field_name, self._client)
             for field_name, data in self.__dict__.items()
             if not field_name[0] == "_"])

        table = f"""<table>
<thead>
  <tr>
    <th>Field</th>
    <th>Collection</th>
    <th>Document</th>
  </tr>
</thead>
<tbody>
{rows}
</tbody>
</table>"""

        return table

    def set_filters(self, df_l):
        self.project_names = ListRecord(record=set_filters(df_l), collection="Data")

    def set_input_fields(self, date_FTU0, date_FTU1, x_d):
        self.date_FTU0 = Record(date_FTU0, collection='Data')
        self.date_FTU1 = Record(date_FTU1, collection='Data')
        self.x_d = DateRecord(x_d, collection="Data")

    def prognose(self, df_l, start_time, timeline, total_objects, date_FTU0):
        print("Prognose")
        results = prognose(df_l, start_time, timeline, total_objects, date_FTU0)
        self.rc1 = ListRecord(results[0], collection='Data')
        self.rc2 = ListRecord(results[1], collection='Data')
        d_real_l_r = {k: v["Aantal"] for k, v in results[2].items()}
        self.d_real_l_r = ListRecord(d_real_l_r, collection="Data")
        d_real_l_ri = {k: v.index for k, v in results[2].items()}
        self.d_real_l_ri = ListRecord(d_real_l_ri, collection="Data")
        self.y_prog_l = ListRecord(results[3], collection='Data')
        self.x_prog = IntRecord(results[4], collection='Data')
        self.t_shift = StringRecord(results[5], collection='Data')
        self.cutoff = Record(results[6], collection='Data')
        return results

    def targets(self, x_prog, timeline, t_shift, date_FTU0, date_FTU1, rc1, d_real_l):
        print("Targets")
        results = targets(x_prog, timeline, t_shift, date_FTU0, date_FTU1, rc1, d_real_l)
        self.y_target_l = ListRecord(results[0], collection='Data')
        return results

    def error_check_FCBC(self, df_l):
        print("Error check")
        results = error_check_FCBC(df_l)
        self.n_err = Record(results[0], collection='Data')
        self.errors_FC_BC = Record(results[1], collection='Data')
        print("error check done")
        return results

    def calculate_projectspecs(self, df_l):
        print("Projectspecs")
        results = calculate_projectspecs(df_l)
        self.HC_HPend = Record(results[0], collection='Data')
        self.HC_HPend_l = Record(results[1], collection='Data')
        self.Schouw_BIS = Record(results[2], collection='Data')
        self.HPend_l = Record(results[3], collection='Data')
        self.HAS_werkvoorraad = Record(results[4], collection='Data')
        return results

    def calculate_graph_overview(self, df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad):
        graph_targets_W = graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad, res='W-MON')
        graph_targets_M, jaaroverzicht = graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend,
                                                        HAS_werkvoorraad, res='M')
        self.graph_targets_W = Record(graph_targets_W, collection='Graphs')
        self.graph_targets_M = Record(graph_targets_M, collection='Graphs')
        self.jaaroverzicht = Record(jaaroverzicht, collection='Data')

    def performance_matrix(self, x_d, y_target_l, d_real_l, tot_l, t_diff, y_voorraad_act):
        graph = performance_matrix(x_d, y_target_l, d_real_l, tot_l, t_diff, y_voorraad_act)
        self.project_performance = Record(graph, collection="Graphs")

    def calculate_y_voorraad_act(self, df_l):
        results = calculate_y_voorraad_act(df_l)
        self.y_voorraad_act = Record(results, collection='Data')
        return results

    def prognose_graph(self, x_d, y_prog_l, d_real_l, y_target_l):
        result_dict = prognose_graph(x_d, y_prog_l, d_real_l, y_target_l)
        self.prognose_graph_dict = DictRecord(result_dict, collection="Graphs")

    def info_table(self, tot_l, d_real_l, HP, y_target_l, x_d, HC_HPend_l, Schouw_BIS, HPend_l, n_err):
        record = info_table(tot_l, d_real_l, HP, y_target_l, x_d, HC_HPend_l, Schouw_BIS, HPend_l, n_err)
        self.info_table = Record(record, collection="Graphs")

    def reden_na(self, df_l, clusters):
        overview_record = overview_reden_na(df_l, clusters)
        record_dict = individual_reden_na(df_l, clusters)
        self.reden_na_overview = Record(overview_record, collection="Graphs")
        self.reden_na_projects = DictRecord(record_dict, collection="Graphs")

    def to_firestore(self):
        for field_name, data in self.__dict__.items():
            if not field_name[0] == "_":
                try:
                    data.to_firestore(graph_name=field_name, client=self._client)
                    print(f"Wrote {field_name} to firestore")
                except TypeError:
                    print(f"Could not write {field_name} to firestore.")
