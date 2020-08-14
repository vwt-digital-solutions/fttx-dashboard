from data import api


def figure_data(graph_id):
    return api.get(f'/Graphs?id={graph_id}')[0]['figure']
