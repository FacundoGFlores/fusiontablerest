import pypyodbc
import csv

class SQLConnector:
    def __init__(
        self, server, uid, pwd, dbname
    ):
        connection_string = "DRIVER={SQL Server};"
        connection_string += "SERVER=" + server + ";"
        connection_string += "DATABASE=" + dbname + ";"
        connection_string += "UID=" + uid + ";"
        connection_string += "PWD=" + pwd + ";"
        self.connection = pypyodbc.connect(connection_string)
        self.cursor = self.connection.cursor()
        self.headerinfo = None
        print "SQLConnector Cursor Ready!"

    def toText(s):
        return "'"+s+"'"

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
        print "Executing query"
        self.cursor.execute(query)
        self.headerinfo = self.cursor.description

    def getRows(self):
        print "Getting rows from db"
        return self._query()["results"]

    def getColumns(self):
        print "Getting columns"
        return [column[0] for column in self.cursor.description]

    def getHeaderInfo(self):
        return self.headerinfo

    def writeCSV(self, filename, include_headers=True):
        print "Writing CSV"
        with open(filename + ".csv", "w") as f:
            if include_headers:
                csv.writer(f).writerow(
                    [d[0] for d in self.cursor.description]
                )
            csv.writer(f).writerows(self.cursor)
            print "CSV wrote!"
