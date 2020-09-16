from Analyse.Record import Record, IntRecord, DateRecord, ListRecord, StringRecord, DocumentListRecord
import logging


class TestRecord:
    def test_record(self):
        data = "Data"
        collection = "Graphs"
        record = Record(record=data, collection=collection)
        assert record._validate(data)
        assert record._transform(data) == data
        assert record._record == data
        assert record.collection == collection

    def test_int_record(self):
        data = "1234"
        collection = "Graphs"
        record = IntRecord(record=data, collection=collection)
        assert record._validate(data)
        assert record._transform(data) == [1, 2, 3, 4]
        assert record._record == [1, 2, 3, 4]
        assert record.collection == collection

    def test_date_record(self):
        from datetime import date

        data = [date(2020, 8, 20)]
        data_transformed = ['2020-08-20']
        collection = "Graphs"
        record = DateRecord(record=data, collection=collection)
        assert record._validate(data)
        assert record._transform(data) == data_transformed
        assert record._record == data_transformed
        assert record.collection == collection

    def test_list_record(self):
        data = {'item1': 'item1', 'item2': 'item2'}
        data_transformed = {'item1': ['i', 't', 'e', 'm', '1'], 'item2': ['i', 't', 'e', 'm', '2']}

        collection = "Graphs"
        record = ListRecord(record=data, collection=collection)
        assert record._validate(data)
        assert record._transform(data) == data_transformed
        assert record._record == data_transformed
        assert record.collection == collection

    def test_string_record(self):
        data = {'item1': 1234, 'item2': 123.4}
        data_transformed = {'item1': '1234', 'item2': '123.4'}

        collection = "Graphs"
        record = StringRecord(record=data, collection=collection)
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
        record = DocumentListRecord(record=data, collection=collection)
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
        record = DocumentListRecord(record=data, collection=collection, document_key=['filter'])
        record._validate(data)
        assert "There is no field 'record' in document with index" not in caplog.text
        assert "There is no field 'filter' in document with index" not in caplog.text
        assert record._validate(data)

        # Transform does not process the data, but the validation fails so the setter skips setting the value.
        assert record._transform(data) == data
        assert record._record == data
        assert record.collection == collection
