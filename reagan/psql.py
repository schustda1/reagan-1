#!/usr/bin/python
from reagan.subclass import Subclass
from sqlalchemy import create_engine
import pandas as pd
import psycopg2

class PSQL(Subclass):

    def __init__(self, server, verbose=0):
        super().__init__()
        self.verbose = verbose
        self.server = server
        self.connection_string = eval(self.get_parameter_value(f"/postgres/{self.server}"))
        self._connect_to_database(self.connection_string)

    def _connect_to_database(self,connection):
        '''
        Function called when generating an instance of the class that
        opens the database connection.
        '''

        self.conn = create_engine('''{engine}+psycopg2://{user}:{password}@{host}:{port}/{dbname}'''.format(**connection), connect_args={'sslmode':'prefer'})

    def to_df(self,query_input,replacements={}):
        '''
        Takes in a string containing either a correctly formatted SQL
        query, or filepath directed to a .sql file. Returns a pandas DataFrame
        of the executed query.
        '''

        query = self._format_query(query_input,replacements)
        if self.verbose:
            print ('Executing Query:\n\n',format(query,reindent=True,keyword_case='upper'))
        return pd.read_sql(query,self.conn)

    def to_dict(self, schema, table, key, value):
        """
        Executes query and returns results into a dict via sqlalchemy
            - schema (string): schema of the table to pull
            - table (string): table name of the table to pull
            - key (string): column name in the table to be used as the dictionary key
            - value (string): column name in the table to be used as the dictionary value
        """
        df = self.to_df(f'''SELECT DISTINCT {key} AS key, {value} AS value FROM {schema}.{table}''')
        return {row["key"]: row["value"] for idx, row in df.iterrows()}

    def to_list(self, query):
        """
        Executes query and returns results into a list via sqlalchemy
            - query (string): The SQL query to execute. Should be a SELECT
        """
        df = self.to_df(query)
        return df.values.flatten().tolist()

    def execute(self, query, replacements={}):
        """
        Executes query and returns results into a pandas dataframe via sqlalchmey
            - query (string): The SQL query to execute. Should be a SELECT
            - replacements (dict): Modifies the query and replaces any instance of [key] with [value]
        """
        query = self._format_query(query, replacements)
        with self.conn.connect() as con:
            con.execute(query)

    def to_sql(self, df, schema, table, if_exists='fail', index = False, chunksize = 1000):
        '''
        Inserts a pandas dataframe into the specified table in DADL.
        Types accepted are 'append' and 'replace'
        '''

        df.to_sql(
            name=table,
            if_exists=if_exists,
            schema=schema,
            index=index,
            con=self.conn,
            chunksize=chunksize,
        )

    def get_scalar(self, query, replacements={}):
        """
        Executes query and returns a single result
            - query (string): The SQL query to execute. Should be a SELECT
            - replacements (dict): Modifies the query and replaces any instance of [key] with [value]
        """
        query = self._format_query(query, replacements)
        return self.conn.scalar(query)

if __name__ == "__main__":
    q = PSQL('scp')
    q = PSQL('george')
    q = PSQL('pdw_gm_o')
    # q2 = '''DELETE FROM items.parameters WHERE True'''
    # q1 = '''INSERT INTO items.parameters (name, value_int) VALUES ('max_post_number', 153009616)'''
    # q.execute(q2)
    # a = q.to_list('SELECT value_int FROM items.parameters')
    # b = q.to_dict(schema = 'items', table = 'parameters', key='name',value = 'value_int')
    # c = q.get_scalar('SELECT MAX(value_int) FROM items.parameters')
    # df = p.to_df('''SELECT * FROM carat_gm.dcm_date limit 1000''')