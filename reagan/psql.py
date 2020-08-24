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
        self.connection_string = eval(self.get_parameter("postgres").get_parameter(self.server))
        self._connect_to_database(self.connection_string)

    def _connect_to_database(self,connection):
        '''
        Function called when generating an instance of the class that
        opens the database connection.
        '''

        # hard coding b/c I don't care
        if self.server in ['george','scp']:
            # get a connection, if a connect cannot be made an exception will be raised here
            self.conn = psycopg2.connect(**connection)
            # conn.cursor will return a cursor object, you can use this cursor to perform queries
            self.cursor = self.conn.cursor()
        else:
            self.conn = create_engine('''redshift+psycopg2://{user}:{password}@{host}:{port}/{dbname}'''.format(**connection), connect_args={'sslmode':'prefer'})


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

if __name__ == "__main__":
    q = PSQL('pdw_gm_o')
    p = PSQL('george')
    # df = p.to_df('''SELECT * FROM carat_gm.dcm_date limit 1000''')