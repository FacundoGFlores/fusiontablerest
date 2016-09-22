#!/usr/bin/env python
"""
    This class allow us to interact with SQL Server.
    Remember to setup the dbconnection.json file
"""

import csv
import logging
import sys

import pypyodbc

__author__ = "Flores, Facundo Gabriel"

__version__ = "0.1.1"
__maintainer__ = "Flores, Facundo Gabriel"
__email__ = "flores.facundogabriel@gmail.com"
__status__ = "Development"


class SQLConnector:

    def __init__(
        self, server, uid, pwd, dbname
    ):
        try:
            connection_string = "DRIVER={SQL Server};"
            connection_string += "SERVER=" + server + ";"
            connection_string += "DATABASE=" + dbname + ";"
            connection_string += "UID=" + uid + ";"
            connection_string += "PWD=" + pwd + ";"
            self.connection = pypyodbc.connect(connection_string)
            self.cursor = self.connection.cursor()
            self.headerinfo = None
            logging.info("SQLConnector Cursor Ready!")
        except:
            logging.error("Unable to connect to the SQL SERVER Database")
            sys.exit(1)

    def toText(s):
        return "'" + s + "'"

    def _query(self):
        return {
            'results': [
                dict(
                    zip(
                        [column[0] for column in self.cursor.description],
                        row
                    )
                )
                for row in self.cursor.fetchall()
            ]
        }

    def getPK(self, tablename):
        # Remember it could be more than one key
        return self.cursor.primaryKeys(tablename).fetchone()[3].lower()

    def runSQLQuery(self, query):
        logging.info("Executing query")
        try:
            self.cursor.execute(query)
            self.headerinfo = self.cursor.description
        except:
            logging.error("Unable to get cursor")
            sys.exit(1)

    def getRows(self):
        logging.info("Getting rows from db")
        try:
            return self._query()["results"]
        except:
            logging.error("Unable to get rows")
            sys.exit(1)

    def getColumns(self):
        logging.info("Getting columns")
        try:
            return [column[0] for column in self.cursor.description]
        except:
            logging.error("Unable to get columns")
            sys.exit(1)

    def getHeaderInfo(self):
        try:
            return self.headerinfo
        except:
            logging.error("Unable to get header info")
            sys.exit(1)

    def writeCSV(self, filename, include_headers=True):
        logging.info("Writing CSV...")
        try:
            with open(filename + ".csv", "w") as f:
                if include_headers:
                    csv.writer(f, lineterminator="\n").writerow(
                        [d[0] for d in self.cursor.description]
                    )
                csv.writer(f, lineterminator="\n").writerows(self.cursor)
                logging.info("CSV wrote!")
        except:
            logging.error("Unable to write csv file")
            sys.exit(1)
