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

    def runSQLQuery(self, query):
        self.cursor.execute(query)

    def getRows(self):
        return self._query()["results"]

    def getColumns(self):
        return [column[0] for column in self.cursor.description]

    def getHeader(self):
        return self.cursor.description

    def writeCSV(self, filename, include_headers=True):
        with open(filename + ".csv", "w") as f:
            if include_headers:
                csv.writer(f).writerow(
                    [d[0] for d in self.cursor.description]
                )
            csv.writer(f).writerows(self.cursor)
