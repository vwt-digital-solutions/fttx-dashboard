try:
    from functions import pie_chart_reden_na, get_pie_layout
except ImportError:
    from analyse.functions import pie_chart_reden_na, get_pie_layout


# Small change to functions for KPN to work with dataframe instead of dict
# Should be generalised in later stage
def overview_reden_na_df(df, clusters):
    data, document = pie_chart_reden_na(df, clusters, 'overview')
    layout = get_pie_layout()
    fig = {
        'data': data,
        'layout': layout
    }
    record = dict(id=document, figure=fig)
    return record


# Small change to functions for KPN to work with dataframe instead of dict
# Should be generalised in later stage
def individual_reden_na_df(project_data, clusters):
    record_dict = {}
    for project in project_data.project.unique():
        project_data = project_data[project_data.project == project]
        data, document = pie_chart_reden_na(project_data, clusters, project)
        layout = get_pie_layout()
        fig = {
            'data': data,
            'layout': layout
        }
        record = dict(id=document, figure=fig)
        record_dict[document] = record
    return record_dict
