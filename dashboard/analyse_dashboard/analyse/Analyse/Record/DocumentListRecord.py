import logging

from google.cloud import firestore

from Analyse.Record.Record import Record, Validation


class DocumentListRecord(Record):
    """A list of dictionaries to be written as separate documents, no further manipulation needed. When  """

    def __init__(self, record, collection, client, graph_name, document_key=None):
        if document_key is None:
            document_key = ['id']
        self.document_key = document_key
        super().__init__(record, collection, client, graph_name)

    def _validate(self, value):
        validated = super()._validate(value)
        for i, doc in enumerate(value):
            if "record" not in doc:
                logging.warning(f"There is no field 'record' in document with index {i}")
                validated = False
                if self._validation == Validation.FAIL_AT_FIRST:
                    break

            if not self.document_key:
                logging.warning("You have no document key configured. This is needed to create a unique document name.")
                validated = False
                if self._validation == Validation.FAIL_AT_FIRST:
                    break

            for key_part in self.document_key:
                if key_part not in doc:
                    logging.warning(f'''There is no field '{key_part}' in document with index {i}.
                                        Your document must be uniquely identifiable''')
                    validated = False
                    if self._validation == Validation.FAIL_AT_FIRST:
                        break
        return validated

    def document_name(self, document=None):
        if document:
            doc_name_parts = [self.client, self.graph_name] + [document[key_part] for key_part in self.document_key]
            document_name = "_".join(part for part in doc_name_parts if part)
            return document_name
        return super().document_name()

    def to_firestore(self, graph_name=None, client=""):
        if not self.record:
            return

        logging.info(f"Creating documents for {graph_name} in DocumentListRecord")
        db = firestore.Client()
        batch = db.batch()
        for i, document in enumerate(self.record):
            if client and "client" not in document:
                document['client'] = client
            document_name = self.document_name(document=document)
            fs_document = db.collection(self.collection).document(document_name)
            logging.info(f"Set document {document_name}")
            batch.set(fs_document, document)
            if not i % 100:
                batch.commit()
                batch = db.batch()
        batch.commit()

    def to_table_part(self, graph_name="", client=""):
        table_part = ""
        for doc in self.record:
            document_name = self.document_name(document=doc)
            table_part += f"""<tr>
              <td>{document_name}</td>
              <td>{self.collection}</td>
              <td>{doc}</td>
            </tr>"""
        return table_part
