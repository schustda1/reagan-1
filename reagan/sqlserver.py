#!/usr/bin/python
from reagan.subclass import Subclass
import pandas as pd
import pyodbc
import sqlalchemy
import urllib


class SQLServer(Subclass):
    def __init__(self, server):
        super().__init__()
        self.server = server
        self._connect_to_database(self.get_parameter("sqlserver").get_parameter(self.server))

    def _connect_to_database(self, connection):

        # Connect via pyodbc. Used for DML statements
        self.conn = pyodbc.connect(connection)

        # conn.cursor will return a cursor object, you can use this cursor to perform queries
        self.cursor = self.conn.cursor()

        # Connect via sqlalchemy to perform interactions with the pandas
        conn_string = "mssql+pyodbc:///?odbc_connect={}".format(
            urllib.parse.quote_plus(connection)
        )
        self.engine = sqlalchemy.create_engine(
            conn_string, echo=False, fast_executemany=True
        )

    def to_df(self, query, replacements={}, table=None):
        """
        Executes query and returns results into a pandas dataframe via sqlalchemy
            - query (string): The SQL query to execute. Should be a SELECT
            - replacements (dict): Modifies the query and replaces any instance of [key] with [value]
        """
        if table:
            query = f'SELECT * FROM {table}'
        else:
            query = self._format_query(query, replacements)
        return pd.read_sql(query, self.conn)

    def to_list(self, query):
        """
        Executes query and returns results into a list via sqlalchemy
            - query (string): The SQL query to execute. Should be a SELECT
        """
        df = self.to_df(query)
        return df.values.flatten().tolist()

    def to_dict(self, schema, table, key, value):
        """
        Executes query and returns results into a dict via sqlalchemy
            - schema (string): schema of the table to pull
            - table (string): table name of the table to pull
            - key (string): column name in the table to be used as the dictionary key
            - value (string): column name in the table to be used as the dictionary value
        """
        df = self.to_df(
            f"SELECT DISTINCT {key} AS [key], {value} AS [value] FROM {schema}.{table}"
        )
        return {row["key"]: row["value"] for idx, row in df.iterrows()}

    def to_sql(self, df, name, schema, if_exists="fail", index=False, chunksize=1000):
        """
        Inserts a pandas dataframe into the specified table in DADL.
            - df (DataFrame): The data which to insert into the SQL table.
            - name (string): The table name to insert data into.
            - schema (string): The schema location of the table.
            - if_exists (string): How to treat the insertion. Accepted values are: 'fail','append','replace' (default is fail).
                - If 'replace' is selected, the table will be dropped and recreated with auto-assigned datatypes.
            - index (string): Whether to include the DataFrame index in the table (default is False).
            - chunksize (int): Chunksize will determine how many rows are written to the database at a time.
        """

        df.to_sql(
            name=name,
            if_exists=if_exists,
            schema=schema,
            index=index,
            con=self.engine,
            chunksize=chunksize,
        )

    def execute(self, query, replacements={}):
        """
        Executes query and returns results into a pandas dataframe via sqlalchmey
            - query (string): The SQL query to execute. Should be a SELECT
            - replacements (dict): Modifies the query and replaces any instance of [key] with [value]
        """
        query = self._format_query(query, replacements)
        try:
            self.cursor.execute(query)
        except pyodbc.DatabaseError as e:
            print(e)
            self.conn.rollback()
        else:
            self.conn.commit()

    def get_scalar(self, query, replacements={}):
        """
        Executes query and returns a single result
            - query (string): The SQL query to execute. Should be a SELECT
            - replacements (dict): Modifies the query and replaces any instance of [key] with [value]
        """
        query = self._format_query(query, replacements)
        return self.engine.scalar(query)


if __name__ == "__main__":
    ss = SQLServer("102")
    sites = ss.to_dict(
        schema="glops", table="DCM_Sites", key="Site_Id_DCM", value="Site_DCM"
    )

