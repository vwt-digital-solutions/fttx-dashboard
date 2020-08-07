from data import api


def jaaroverzicht_data(value):
    return api.get('/Graphs?id=jaaroverzicht')[0][value]
