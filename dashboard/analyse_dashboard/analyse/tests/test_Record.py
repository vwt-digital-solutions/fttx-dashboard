from Analyse.Capacity_analysis.Line import PointLine
from Analyse.Record.LineRecord import LineRecord
from Analyse.Record.IntRecord import IntRecord
from Analyse.Record.DateRecord import DateRecord
from Analyse.Record.ListRecord import ListRecord
from Analyse.Record.StringRecord import StringRecord
from Analyse.Record.DocumentListRecord import DocumentListRecord
from Analyse.Record.Record import Record
import pandas as pd
import logging


class TestRecord:
    def test_record(self):
        data = "Data"
        collection = "Graphs"
        client = 'kpn'
        graph_name = 'test'
        record = Record(record=data, collection=collection, client=client, graph_name=graph_name)
        assert record._validate(data)
        assert record._transform(data) == data
        assert record._record == data
        assert record.collection == collection

    def test_int_record(self):
        data = "1234"
        collection = "Graphs"
        client = 'kpn'
        graph_name = 'test'
        record = IntRecord(record=data, collection=collection, client=client, graph_name=graph_name)
        assert record._validate(data)
        assert record._transform(data) == [1, 2, 3, 4]
        assert record._record == [1, 2, 3, 4]
        assert record.collection == collection

    def test_date_record(self):
        from datetime import date

        data = [date(2020, 8, 20)]
        data_transformed = ['2020-08-20']
        collection = "Graphs"
        client = 'kpn'
        graph_name = 'test'
        record = DateRecord(record=data, collection=collection, client=client, graph_name=graph_name)
        assert record._validate(data)
        assert record._transform(data) == data_transformed
        assert record._record == data_transformed
        assert record.collection == collection

    def test_list_record(self):
        data = {'item1': 'item1', 'item2': 'item2'}
        data_transformed = {'item1': ['i', 't', 'e', 'm', '1'], 'item2': ['i', 't', 'e', 'm', '2']}

        collection = "Graphs"
        client = 'kpn'
        graph_name = 'test'
        record = ListRecord(record=data, collection=collection, client=client, graph_name=graph_name)
        assert record._validate(data)
        assert record._transform(data) == data_transformed
        assert record._record == data_transformed
        assert record.collection == collection

    def test_string_record(self):
        data = {'item1': 1234, 'item2': 123.4}
        data_transformed = {'item1': '1234', 'item2': '123.4'}

        collection = "Graphs"
        client = 'kpn'
        graph_name = 'test'
        record = StringRecord(record=data, collection=collection, client=client, graph_name=graph_name)
        assert record._validate(data)
        assert record._transform(data) == data_transformed
        assert record._record == data_transformed
        assert record.collection == collection

    def test_document_list_record_fail(self, caplog):
        caplog.set_level(logging.WARNING)

        data = [
            dict(project="proj1", graph_name="graph1", filter="filter1", record=dict(value1="some data")),
            dict(project="proj1", graph_name="graph1", filter="filter2", record=dict(value1="some other data")),
            dict(project="proj2", graph_name="graph1", filter="filter1"),
            dict(project="proj2", graph_name="graph1", filter="filter2", record=dict(value1="some other data"))
        ]
        data_transformed = None

        collection = "Graphs"
        client = 'kpn'
        graph_name = 'test'
        record = DocumentListRecord(record=data, collection=collection, client=client, graph_name=graph_name)
        record._validate(data)
        assert "There is no field 'record' in document with index 2" in caplog.text
        assert "There is no field 'id' in document with index" in caplog.text
        assert not record._validate(data)

        # Transform does not process the data, but the validation fails so the setter skips setting the value.
        assert record._transform(data) == data
        assert record._record == data_transformed
        assert record.collection == collection

    def test_document_list_record_succeed(self, caplog):
        caplog.set_level(logging.WARNING)

        data = [
            dict(project="proj1", graph_name="graph1", filter="filter1", record=dict(value1="some data")),
            dict(project="proj1", graph_name="graph1", filter="filter2", record=dict(value1="some other data")),
            dict(project="proj2", graph_name="graph1", filter="filter2", record=dict(value1="some other data"))
        ]

        collection = "Graphs"
        client = 'kpn'
        graph_name = 'test'
        record = DocumentListRecord(record=data, collection=collection, document_key=['filter'],
                                    client=client, graph_name=graph_name)
        record._validate(data)
        assert "There is no field 'record' in document with index" not in caplog.text
        assert "There is no field 'filter' in document with index" not in caplog.text
        assert record._validate(data)

        # Transform does not process the data, but the validation fails so the setter skips setting the value.
        assert record._transform(data) == data
        assert record._record == data
        assert record.collection == collection

    def test_line_record(self):
        data = PointLine(pd.Series([1, 2, 3, 4, 5]))
        collection = 'Lines'
        client = 'kpn'
        graph_name = 'test'
        phase = 'lasap'
        record = LineRecord(record=data, collection=collection, client=client, graph_name=graph_name, phase=phase)

        assert record._validate(data)
        assert record._transform(data) == data
