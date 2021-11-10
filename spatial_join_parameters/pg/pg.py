"""
Define functions to organize postgresql connection.
"""
from __future__ import unicode_literals
import sys
sys.path.append('./classes')
import psycopg2

class db_connector():
    """
    Class db_connector provides helper functions for managing connections to a postgresql database.
    """

    def __init__(self):
        self.dbConnector = {"error" : None,  "connection" : None,  "query_result" : None}

    def dbConfig(self,  db_config):
        """
        Set database configuration

        Parameters
        ----------
        db_config : dict
            dictionary filled with parameters to get access to database:
            {
                "db_host" : "192.168.0.193",
                "db_name" : "stb_grow",
                "db_user" : "visdat",
                "db_password" : "9Leravu6",
                "db_port" : "5432"
            }
        """
        self.db_password = db_config['db_password']
        self.db_user = db_config['db_user']
        self.db_port = db_config['db_port']
        self.db_name = db_config['db_name']
        self.db_host = db_config['db_host']

    def dbConnect(self):
        """
        Set database connection string

        Returns
        ----------
        dbConnector : handler
            database connector handler
        """
        try:
            connector = psycopg2.connect("dbname='"+self.db_name+"' user='"+self.db_user+"' host='"+self.db_host+"' port='"+self.db_port+"' password='"+self.db_password+"'")
            self.dbConnector["connection"] = connector
            #print ("OK -> database connection established...")
        except Exception as e:
            self.dbConnector["error"] = e
            print('ERROR: ', self.dbConnector)
            sys.exit()
        return self.dbConnector

    def dbClose(self):
        """
        Close database connection
        """
        try:
            self.dbConnector["connection"].commit()
            self.dbConnector["connection"].close()
            #print("OK -> database connection closed...")
        except Exception as e:
            self.dbConnector["error"] = e

    def tblInsert(self, table,  columns,  insertPlaceholder,  valuesAsJson,  returnParam):
        """
        Insert values into a table

        Parameters
        ----------
        table : text
            table name
        columns : text
            comma separated list of column names
        insertPlaceholder : text
            comma separated list of columns as placeholder: ( %(str)s, %(int)s, %(date)s, ... )
        valuesAsJson : json
            json formatted values: {}
        returnParam : text
            column name of return parameter, e.g. row id

        Returns
        ----------
        value : variable
            value of return parameter
        """
        returnValue =  None
        if returnParam != None:
            sql = "INSERT INTO " + table + "(" + columns + ") " + \
                "VALUES(" + insertPlaceholder + ") RETURNING " + returnParam
        else:
            sql = "INSERT INTO " + table + "(" + columns + ") " + \
                "VALUES(" + insertPlaceholder + ")"
        #print(sql)
        #print(valuesAsJson)
        cur = self.dbConnector["connection"].cursor()
        try:
            cur.execute(sql,  valuesAsJson)
            if returnParam != None:
                returnValue = cur.fetchone()[0]
        except Exception as  e:
            self.dbConnector["error"] = e
            print ("DATABASE ERROR")
            print (e)
            sys.exit()
        cur.close()
        self.dbConnector["connection"].commit()

        return returnValue

    def tblDeleteRows(self,  tblName, whereCondition):
        """Delete rows from a table"""
        if whereCondition != None:
            sql = "DELETE FROM " + tblName + " WHERE " + whereCondition
        else:
            sql =  "DELETE testerror FROM " + tblName
        print (sql)
        cur = self.dbConnector["connection"].cursor()
        try:
            cur.execute(sql)
        except Exception as  e:
            self.dbConnector["error"] = e
            print ("DATABASE ERROR")
            print (e)
        cur.close()
        self.dbConnector["connection"].commit()

    def tblSelect(self, sql):
        """Select data from table by using a sql statement"""
        #print("--> query data from database... ")
        #print (sql)
        queryResult = None
        rowcount = 0
        cur = self.dbConnector["connection"].cursor()
        try:
            cur.execute(sql)
            queryResult = cur.fetchall()
            rowcount = cur.rowcount
        except Exception as  e:
            self.dbConnector["error"] = e
            print (e)
        cur.close()
        #print("--> row count of query result... " + str(rowcount))
        return queryResult,  rowcount
