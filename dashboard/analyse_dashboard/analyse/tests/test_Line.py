from Analyse.Capacity_analysis.Domain import Domain
from Analyse.Capacity_analysis.Line import PointLine, LinearLine
import pandas as pd
import numpy as np


class TestPointLine:
    def test_init(self):
        line = PointLine(pd.Series([1, 2, 3, 4, 5]))
        assert isinstance(line.data, pd.Series)
        line = PointLine([1, 2, 3, 4, 5])
        assert isinstance(line.data, pd.Series)

    def test_mul(self):
        line = PointLine([1, 2, 3, 4, 5])
        line = line * 2
        assert line == PointLine([2, 4, 6, 8, 10])

    def test_imul(self):
        line = PointLine([1, 2, 3, 4, 5])
        line *= 2
        assert line == PointLine([2, 4, 6, 8, 10])

    def test_div(self):
        line = PointLine([2, 4, 6, 8])
        line = line / 2
        assert line == PointLine([1.0, 2.0, 3.0, 4.0])

    def test_idiv(self):
        line = PointLine([2, 4, 6, 8])
        line /= 2
        assert line == PointLine([1.0, 2.0, 3.0, 4.0])

    def test_add(self):
        line = PointLine([1, 2, 3, 4, 5])
        line = line + 2
        assert line == PointLine([3, 4, 5, 6, 7])

    def test_iadd(self):
        line = PointLine([1, 2, 3, 4, 5])
        line += 2
        assert line == PointLine([3, 4, 5, 6, 7])

    def test_sub(self):
        line = PointLine([1, 2, 3, 4, 5])
        line = line - 2
        assert line == PointLine([-1, 0, 1, 2, 3])

    def test_isub(self):
        line = PointLine([1, 2, 3, 4, 5])
        line -= 2
        assert line == PointLine([-1, 0, 1, 2, 3])

    def test_add_line(self):
        line1 = PointLine([1, 2, 3, 4, 5])
        line2 = PointLine([1, 2, 3, 4, 5])
        assert line1 + line2 == PointLine([2, 4, 6, 8, 10])

    def test_sub_line(self):
        line1 = PointLine([1, 2, 3, 4, 5])
        line2 = PointLine([1, 2, 3, 4, 5])
        assert line1 - line2 == PointLine([0, 0, 0, 0, 0])

    def test_mul_line(self):
        line1 = PointLine([1, 2, 3, 4, 5])
        line2 = PointLine([1, 2, 3, 4, 5])
        assert line1 * line2 == PointLine([1, 4, 9, 16, 25])

    def test_div_line(self):
        line1 = PointLine([1, 2, 3, 4, 5])
        line2 = PointLine([1, 2, 3, 4, 5])
        assert line1 / line2 == PointLine([1.0, 1.0, 1.0, 1.0, 1.0])

    def test_add_linearline(self):
        line1 = PointLine([1, 2, 3, 4, 5])
        line2 = LinearLine(slope=1, intercept=1, domain=Domain(begin=0, end=5))
        assert line1 + line2 == PointLine([2, 4, 6, 8, 10])

    def test_sub_linearline(self):
        line1 = PointLine([1, 2, 3, 4, 5])
        line2 = LinearLine(slope=1, intercept=1, domain=Domain(begin=0, end=5))
        assert line1 - line2 == PointLine([0, 0, 0, 0, 0])

    def test_mul_linearline(self):
        line1 = PointLine([1, 2, 3, 4, 5])
        line2 = LinearLine(slope=1, intercept=1, domain=Domain(begin=0, end=5))
        assert line1 * line2 == PointLine([1, 4, 9, 16, 25])

    def test_div_linearline(self):
        line1 = PointLine([1, 2, 3, 4, 5])
        line2 = LinearLine(slope=1, intercept=1, domain=Domain(begin=0, end=5))
        assert line1 / line2 == PointLine([1.0, 1.0, 1.0, 1.0, 1.0])

    def test_diff_pointline(self):
        line1 = PointLine([1, 3, 5, 3, 6])
        assert line1.differentiate() == PointLine([np.nan, 2, 2, -2, 3])
