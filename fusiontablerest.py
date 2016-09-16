from apiclient.discovery import build
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
        sql_query = "SELECT * FROM " + fusiontable_id
        request = self.service.query().sql(sql=sql_query)
        fusiontable = request.execute()
        return fusiontable["rows"]

    def getColumns(self, fusiontable_id):
        sql_query = "SELECT * FROM " + fusiontable_id
        request = self.service.query().sql(sql=sql_query)
        fusiontable = request.execute()
        return fusiontable["columns"]
