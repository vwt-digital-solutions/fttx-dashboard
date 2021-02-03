from Analyse.Capacity_analysis.PhaseCapacity.PhaseCapacity import PhaseCapacity
from Analyse.Capacity_analysis.Line import TimeseriesLine


class LasDPCapacity(PhaseCapacity):
    def __init__(self, werkvoorraad, **kwargs):
        super().__init__(**kwargs)
        self.werkvoorraad = werkvoorraad
        self.phase = 'lasdp'

    def algorithm(self):
        super().algorithm()
        objects = []
        objects += self._werkvoorraad2object()
        objects += self._werkvoorraadabsoluut2record()
        [self.obj_2record(obj) for obj in objects]

    def _werkvoorraad2object(self):
        ratio = self.phase_data[self.phase]['total_units'] / \
                self.phase_data[self.phases_config['master_phase']]['total_units']
        lineobject = TimeseriesLine(data=self.werkvoorraad) * ratio
        lineobject.name = 'werkvoorraad_indicator'
        return lineobject

    def _werkvoorraadabsoluut2object(self):
        werkvoorraadline_object = self._werkvoorraad2object()
        pocidealline_object = self._pocideal2object()
        lineobject = werkvoorraadline_object.integrate() - pocidealline_object.integrate()
        lineobject.name = 'werkvoorraad_absoluut_indicator'
        return lineobject
