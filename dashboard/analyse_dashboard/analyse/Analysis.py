try:
    from functions import prognose, targets, error_check_FCBC, calculate_projectspecs, graph_overview, \
        performance_matrix, \
        calculate_y_voorraad_act, prognose_graph, info_table, overview_reden_na, individual_reden_na, set_filters
    from Record import Record, ListRecord, StringRecord, DateRecord, IntRecord, DictRecord
except ImportError:
    from analyse.functions import prognose, targets, error_check_FCBC, calculate_projectspecs, graph_overview, \
        performance_matrix, \
        calculate_y_voorraad_act, prognose_graph, info_table, overview_reden_na, individual_reden_na, set_filters
    from analyse.Record import Record, ListRecord, StringRecord, DateRecord, IntRecord, DictRecord, RecordDict


class Analysis:

    def __init__(self, client):
        self.client = client
        self.record_dict = RecordDict()

    def __repr__(self):

        return f"Analysis(client={self.client})"

    def __str__(self):

        fields = [field_name for field_name, data in self.record_dict.items()]

        return f"Analysis(client={self.client}) containing: {fields}"

    def _repr_html_(self):
        rows = "\n".join(
            [data.to_table_part(field_name, self.client)
             for field_name, data in self.record_dict.items()
             ])

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

    def to_firestore(self):
        self.record_dict.to_firestore(self.client)


class AnalysisKPN(Analysis):

    def set_input_fields(self, date_FTU0, date_FTU1, x_d):
        self.date_FTU0 = Record(date_FTU0, collection='Data')
        self.date_FTU1 = Record(date_FTU1, collection='Data')
        self.x_d = DateRecord(x_d, collection="Data")

    def prognose(self, df_l, start_time, timeline, total_objects, date_FTU0):
        print("Prognose")
        results = prognose(df_l, start_time, timeline, total_objects, date_FTU0)

        self.record_dict.add('rc1', results[0], ListRecord, 'Data')
        self.record_dict.add('rc2', results[1], ListRecord, 'Data')
        d_real_l_r = {k: v["Aantal"] for k, v in results[2].items()}
        self.record_dict.add('d_real_l_r', d_real_l_r, ListRecord, 'Data')
        d_real_l_ri = {k: v.index for k, v in results[2].items()}
        self.record_dict.add('d_real_l_ri', d_real_l_ri, ListRecord, 'Data')
        self.record_dict.add('y_prog_l', results[3], ListRecord, 'Data')
        self.record_dict.add('x_prog', results[4], IntRecord, 'Data')
        self.record_dict.add('t_shift', results[5], StringRecord, 'Data')
        self.record_dict.add('t_shift', results[6], Record, 'Data')

        return results

    def targets(self, x_prog, timeline, t_shift, date_FTU0, date_FTU1, rc1, d_real_l):
        print("Targets")
        results = targets(x_prog, timeline, t_shift, date_FTU0, date_FTU1, rc1, d_real_l)

        self.record_dict.add('y_target_l', results[0], ListRecord, 'Data')

        return results

    def error_check_FCBC(self, df_l):
        results = error_check_FCBC(df_l)

        self.record_dict('n_err', results[0], Record, 'Data')
        self.record_dict('errors_FC_BC', results[1], Record, 'Data')

        return results

    def calculate_projectspecs(self, df_l):
        print("Projectspecs")
        results = calculate_projectspecs(df_l)

        self.record_dict.add('HC_HPend', results[0], Record, 'Data')
        self.record_dict.add('HC_HPend_l', results[1], Record, 'Data')
        self.record_dict.add('Schouw_BIS', results[2], Record, 'Data')
        self.record_dict.add('HPend_l', results[3], Record, 'Data')
        self.record_dict.add('HAS_werkvoorraad', results[4], Record, 'Data')

        return results

    def calculate_graph_overview(self, df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad):
        graph_targets_W = graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend, HAS_werkvoorraad, res='W-MON')
        graph_targets_M, jaaroverzicht = graph_overview(df_prog, df_target, df_real, df_plan, HC_HPend,
                                                        HAS_werkvoorraad, res='M')

        self.record_dict.add('graph_targets_W', graph_targets_W, Record, 'Graphs')
        self.record_dict.add('graph_targets_M', graph_targets_M, Record, 'Graphs')
        self.record_dict.add('jaaroverzicht', jaaroverzicht, Record, 'Graphs')

    def performance_matrix(self, x_d, y_target_l, d_real_l, tot_l, t_diff, y_voorraad_act):
        graph = performance_matrix(x_d, y_target_l, d_real_l, tot_l, t_diff, y_voorraad_act)
        self.record_dict.add('project_performance', graph, Record, 'Graphs')

    def calculate_y_voorraad_act(self, df_l):
        results = calculate_y_voorraad_act(df_l)

        self.record_dict.add('y_voorraad_act', results, Record, 'Data')

        return results

    def prognose_graph(self, x_d, y_prog_l, d_real_l, y_target_l):
        result_dict = prognose_graph(x_d, y_prog_l, d_real_l, y_target_l)
        self.record_dict.add('prognose_graph_dict', result_dict, DictRecord, 'Graphs')

    def info_table(self, tot_l, d_real_l, HP, y_target_l, x_d, HC_HPend_l, Schouw_BIS, HPend_l, n_err):
        record = info_table(tot_l, d_real_l, HP, y_target_l, x_d, HC_HPend_l, Schouw_BIS, HPend_l, n_err)
        self.record_dict.add('info_table', record, Record, 'Graphs')

    def reden_na(self, df_l, clusters):
        overview_record = overview_reden_na(df_l, clusters)
        record_dict = individual_reden_na(df_l, clusters)
        self.record_dict.add('reden_na_overview', overview_record, Record, 'Graphs')
        self.record_dict.add('reden_na_projects', record_dict, DictRecord, 'Graphs')


class AnalysisTmobile(Analysis):

    def example_analysis(self):
        pass
