import pandas as pd
import logging
import config


class ConsumeAansluitingenHistory:
    """
    Class for consuming Fiberconnect home connections and storing the history
    """

    def __init__(self, records, sql_engine, date=None):
        self.table = config.FC_HISTORY_TABLE
        self.relevant_columns = ['uid', 'sleutel', 'project', 'variable', 'value']
        self.history_columns = config.FC_HISTORY_COLUMNS
        self.records = records
        self.sqlEngine = sql_engine
        self.date = date
        self.reference_records = pd.DataFrame(columns=self.relevant_columns)
        self.records_to_update = pd.DataFrame(columns=self.relevant_columns)
        self.uids_to_upate_versionEnd = []

    def consume_records(self):
        """
        Main function to consume records and store in sql table 'fc_aansluitingen_history'
        """

        self._transform_to_long_format()
        self._read_reference_records()
        self._compare_records()
        self._update_records_versionEnd()
        self._append_new_records()

    def _transform_to_long_format(self):
        """
        Function to keep relevant columns and transform a pd.DataFrame into long format
        """

        logging.info('Transforming DataFrame to long-format ...')
        self.records = self.records[self.history_columns]
        self.records = self.records.melt(id_vars=['sleutel', 'project'])

    def _read_reference_records(self):
        """
        For every record it is checked which records have changed since the last update (normally yesterday).
        In the sql table fc_aansluitingen_history is searched for the reference records to compare with.
        --> reference_record is a record with the same 'sleutel', the same 'variable' and where the 'versionEnd' IS NULL

        These reference_records are store in self.reference_records
        """
        logging.info(f'Reading {len(self.records)} reference records ...')

        limit = 10000
        count = 0
        data = True
        list_of_ids = list(set(self.records.sleutel))

        while data:
            sleutels = list_of_ids[count: count + limit]

            if not sleutels:
                data = False
                logging.info(f'Read {len(self.records)} reference records finished ...')
                break

            sql = f"""
SELECT {",".join(self.relevant_columns)}
FROM {self.table} AS fca
WHERE fca.sleutel IN ({",".join([f"'{sleutel}'" for sleutel in sleutels])})
AND fca.versionEnd IS NULL
"""
            reference_records = pd.read_sql(sql, self.sqlEngine)
            self.reference_records = self.reference_records.append(reference_records)
            count += limit
            if count % 100_000 == 0:
                logging.info(f'Read {count} reference records ...')

    def _compare_records(self):
        """
        Function to compare the reference records with the records to be consumed. The diff needs to be added to the
        sql table `fc_aansluitingen_history`. The matching reference records are now redundant and therefore
        the `versionEnd` of these records need to be updated.

        Stores records to be added as self.records_to_update
        Stores records of which the versionEnd needs to be updated as self.uids_to_update_versionEnd
        """

        if self.reference_records.empty:
            self.records_to_update = self.records
            if self.date:
                self.records_to_update['creationDate'] = pd.Timestamp(self.date).strftime('%Y-%m-%d %H:%M:%S')
            logging.info(f'Found {len(self.records_to_update)} new records')

        else:
            joined = self.records.merge(self.reference_records.drop(columns='uid'), how='left', indicator=True)
            self.records_to_update = joined[joined['_merge'] == 'left_only'].drop(columns='_merge')
            if self.date:
                self.records_to_update['creationDate'] = pd.Timestamp(self.date).strftime('%Y-%m-%d %H:%M:%S')

            joined = self.records_to_update.merge(self.reference_records, on=['sleutel', 'variable'], how='left',
                                                  indicator=True)
            uids_to_update = list(joined[joined['_merge'] == 'both'].uid)
            self.uids_to_upate_versionEnd += uids_to_update
            logging.info(f'Found {len(self.records_to_update)} new records')

    def _update_records_versionEnd(self):
        """
        Function to update the `versionEnd` of given UIDs of records in the sql table `fc_aansluitingen_history`
        """

        if self.uids_to_upate_versionEnd:
            logging.info('Updating versionEnd ...')
            date = f"'{pd.Timestamp(self.date).strftime('%Y-%m-%d %H:%M:%S')}'" if self.date else 'CURRENT_TIMESTAMP'

            sql = f"""
UPDATE {self.table} AS fca
SET fca.versionEnd = {date}
WHERE fca.uid IN ({",".join([f"'{uid}'" for uid in self.uids_to_upate_versionEnd])})
AND fca.versionEnd IS NULL;
"""
            with self.sqlEngine.connect() as con:
                con.execute(sql)

        else:
            logging.info('Skip updating versionEnd, no references where found')

    def _append_new_records(self):
        """
        Function to append the new records in the sql table `fc_aansluitingen_history`. The records are updated in loops
        of 5000
        """
        if not self.records_to_update.empty:
            # Remove None values
            self.records_to_update = self.records_to_update[self.records_to_update.value.notnull()]

            records_available = True
            limit = 5000
            count = 0
            while records_available:
                records = self.records_to_update[count: count + limit]

                if records.empty:
                    records_available = False
                    logging.info(f'Writing {len(self.records_to_update)} reference records to sql finished ...')
                    break

                records.to_sql('fc_aansluitingen_history', con=self.sqlEngine, index=False, if_exists='append')
                count += limit
                if count % 10_000 == 0:
                    logging.info(f'Writing {count} records to sql')


class ConsumeAansluitingen:
    """
    Class for consuming Fiberconnect home connections and storing the min the main sql table fc_aansluitingen
    """

    def __init__(self, records, sql_engine):
        self.table = config.FC_TABLE
        self.records = records
        self.sqlEngine = sql_engine

    def consume_records(self):
        self._append_new_records()

    def _append_new_records(self):
        """
        Function to insert the records in the base table fc_aansluitingen. The records 'sleutel' is used as unique
        identifier. When the id already exists, the record is updated. The records are updated with a max of
        5000 a time.
        """
        logging.info(f'Start writing {len(self.records)} to sql')
        if not self.records.empty:

            records_available = True
            limit = 5_000
            count = 0
            while records_available:
                df = self.records[count: count + limit]

                if df.empty:
                    records_available = False
                    logging.info(f'Writing {len(self.records)} reference records to sql finished ...')
                    break

                columns = ",".join(df.columns)
                values = [tuple(x for x in record) for record in df.values]
                duplicates = ",\n".join(f"{col}=values({col})" for col in df.columns)
                value_question_marks = ",".join(["%s"] * len(df.columns))
                update_query = f"""
INSERT INTO {self.table}
    ({columns})
values
   ({value_question_marks})
on duplicate key update
    {duplicates}
"""

                with self.sqlEngine.connect() as con:
                    con.execute(update_query, *values)

                count += limit
                if count % 10_000 == 0:
                    logging.info(f'Writing {count} records to sql')
