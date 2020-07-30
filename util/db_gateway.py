from config import master_config, db_config

import mysql.connector
from pyhive import presto
from sqlalchemy import create_engine, MetaData, Table


class DbGateway:
    mode = None
    conn = {}
    conf = ''

    def __init__(self):
        self.mode = master_config.MODE
        conf = db_config.getprestoconfig(self.mode)

    # def get_connection(self):
    #     """
    #     function to establish connection with MySQL database
    #     :return: database connection object
    #     """
    #     conf = db_config.getmysqlconfig(self.mode)
    #     db = mysql.connector.connect(host=conf['host'], user=conf['user'], passwd=conf['password'], db=conf['db'])
    #     return db

    def get_connection(self):
        db = presto.connect(host=self.conf['host'], port=self.conf['port'],
                            user=self.conf['user'], passwd=self.conf['password'],
                            db=self.conf['db'])
        return db

    def get_conn(self, name='default'):
        if name not in self.conn:
            dsn, connect_args = self._get_conn_args()
            try:
                engine = create_engine(dsn, connect_args=connect_args)
                self.conn[name] = engine.connect()
            except:
                print("Not able to connect to Presto")

        return self.conn[name]

    @staticmethod
    def _get_conn_args(self):
        ssl_auth = self.conf['password']
        connect_args = {'protocol': 'http'}
        password = ''

        if ssl_auth:
            connect_args = {
                'protocol': 'https',
                'requests_kwargs': {'verify': (self.conf['cert_path'])}
            }
            password = ':' + ssl_auth

        dsn = 'presto://{}{}@{}:{}/{}/{}'.format(self.conf['user'],
                                                 password,
                                                 self.conf['host'],
                                                 self.conf['port'],
                                                 self.conf['catalog'],
                                                 self.conf['schema'])

        return dsn, connect_args



# http://www.swethasubramanian.com/2017/12/29/connecting-to-presto-database-using-python/

