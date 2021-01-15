from Analyse.Capacity_analysis.Line import PointLine
import pandas as pd


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
