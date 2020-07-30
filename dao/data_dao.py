from util import db_gateway
import mysql.connector
import simplejson as json


class DataDao:
    db = None
    conn = None
    cursor = None

    def __init__(self):
        self.db = db_gateway.DbGateway()
        self.conn = self.db.get_conn()
        self.cursor = self.conn.cursor()

    def get(self, query):
        """
        function to return query output
        :param query: final query to be executed with all parameters in place
        :return: data from the query
        """
        count = 1
        try:
            self.cursor.execute(query)
            return self.fetchall()

        except mysql.connector.Error as err:
            print('error')

            if (count <= 5):
                self.conn.reconnect()
                self.cursor = self.conn.cursor()
                self.cursor.execute(query)
                count = count + 1
                return self.fetchall()
            else:
                response = json.dumps({
                    "data": [],
                    "message": "No Data",
                    "status": 204
                })
                return response

    def fetchall(self):
        """
        get all records of the executed query
        :return:
        """
        return self.cursor.fetchall()

    def fetchone(self):
        """
        get first record of the executed query
        :return:
        """
        return self.cursor.fetchone()

    def fetchmany(self,size):
        """
        get multiple records from executed query
        :param size:
        :return:
        """
        return self.cursor.fetchmany(size)

    def get_column_name(self):
        """
        column names of the executed query
        :return:
        """
        return self.cursor.column_names

    def close(self):
        """
        close cursor and connection to DB
        :return:
        """
        if (self.cursor is not None):
            self.cursor.close()

        if (self.conn is not None):
            self.conn.close()
