from data import api


def figure_data(graph_id):
    no_figure = {
            'data': [
            ],
            'layout': {
                'title': 'Graph not found'
            }
        }

    result = api.get(f'/Graphs?id={graph_id}')
    if not result or not len(result):
        return no_figure

    return result[0].get('figure', no_figure)
