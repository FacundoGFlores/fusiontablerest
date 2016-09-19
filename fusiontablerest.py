from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials

class FusionTableREST:
    def __init__(
        self, client_email, p12filename, private_key_password
    ):
        self.client_email = client_email
        self.p12filename = p12filename
        self.private_key_password = private_key_password
        self.scopes = [
            'https://www.googleapis.com/auth/fusiontables',
            'https://www.googleapis.com/auth/fusiontables.readonly'
        ]
        self.credentials = ServiceAccountCredentials.from_p12_keyfile(
            client_email,
            p12filename + '.p12',
            private_key_password,
            self.scopes
        )
        self.http_auth = self.credentials.authorize(Http())
        self.service = build("fusiontables", "v2", http=self.http_auth)

    def getRows(self, fusiontable_id):
        """
            Get rows from fusiontable ID
        """
        sql_query = "SELECT * FROM " + fusiontable_id
        request = self.service.query().sql(sql=sql_query)
        fusiontable = request.execute()
        return fusiontable["rows"]

    def getColumns(self, fusiontable_id):
        """
            Get columns from fusiontable ID
        """
        sql_query = "SELECT * FROM " + fusiontable_id
        request = self.service.query().sql(sql=sql_query)
        fusiontable = request.execute()
        return fusiontable["columns"]

    def _insertRow(self, fusiontable_id, columns, values):
        """
            Insert a row
        """
        sql_query = "INSERT INTO " + fusiontable_id + " "
        sql_query += "(" + ','.join(map(str,columns)) + ") "
        sql_query += "VALUES (" + ','.join(map(str,values)) + ")"
        print sql_query
        request = self.service.query().sql(sql=sql_query)
        res = request.execute()
        return res

    def parseValue(self, odbc_type, val):
        """
            Parse python types for fusiontables
        """
        if odbc_type == str:
            return "'" + val + "'"
        if odbc_type == bool:
            return int(val)
        if odbc_type == int:
            return val

    def insertRowDict(self, fusiontable_id, header, row):
        """
            Insert a row formmated as python dict
        """
        values = []
        columns = []
        for c in header:
            values.append(self.parseValue(c[1], row[c[0]]))
            columns.append(c[0])
        return self._insertRow(fusiontable_id, columns, values)
