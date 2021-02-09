from Analyse.Capacity_analysis.PhaseCapacity.PhaseCapacity import PhaseCapacity
from Analyse.Capacity_analysis.Line import TimeseriesLine


class LasAPCapacity(PhaseCapacity):
    def __init__(self, werkvoorraad, masterphase_data, **kwargs):
        super().__init__(**kwargs)
        self.werkvoorraad = werkvoorraad
        self.masterphase_data = masterphase_data

    def algorithm(self):
        super().algorithm()
        objects = []
        objects += [self.werkvoorraad2object()]
        objects += [self.werkvoorraadabsoluut2object()]
        [self.obj_2record(obj) for obj in objects]
        return self

    def werkvoorraad2object(self):
        ratio = self.phase_data['total_units'] / \
                self.masterphase_data['total_units']
        lineobject = TimeseriesLine(data=self.werkvoorraad) * ratio
        lineobject.name = 'werkvoorraad_indicator'
        return lineobject

    def werkvoorraadabsoluut2object(self):
        werkvoorraadline_object = self.werkvoorraad2object()
        pocidealline_object = self.pocideal2object()
        lineobject = werkvoorraadline_object.integrate() - pocidealline_object.integrate()
        lineobject.name = 'werkvoorraad_absoluut_indicator'
        return lineobject
