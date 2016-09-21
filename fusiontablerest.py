#!/usr/bin/env python
"""
    This class allow us to interact with fusiontables.
    Remember to setup the config.json file
"""

import logging

from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials

__author__ = "Flores, Facundo Gabriel"

__version__ = "0.1.1"
__maintainer__ = "Flores, Facundo Gabriel"
__email__ = "flores.facundogabriel@gmail.com"
__status__ = "Development"


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
        logging.info("Fusion Tables RestAPI Ready!")

    def getRows(self, fusiontable_id, ordercolumn):
        """
            Get rows from fusiontable ID
        """
        logging.info("Getting rows from fusiontable")
        sql_query = "SELECT * FROM " + fusiontable_id
        sql_query += " ORDER BY " + ordercolumn
        request = self.service.query().sql(sql=sql_query)
        fusiontable = request.execute()
        return fusiontable["rows"]

    def getColumns(self, fusiontable_id):
        """
            Get columns from fusiontable ID
        """
        logging.info("Getting columns from fusiontable")
        sql_query = "SELECT * FROM " + fusiontable_id
        request = self.service.query().sql(sql=sql_query)
        fusiontable = request.execute()
        return fusiontable["columns"]

    def cleanTable(self, fusiontable_id):
        logging.warning("Deleting all rows from fusiontable")
        sql_query = "DELETE FROM " + fusiontable_id
        request = self.service.query().sql(sql=sql_query)
        res = request.execute()
        return res

    def _insertRow(self, fusiontable_id, columns, values):
        """
            Insert a row
        """
        sql_query = "INSERT INTO " + fusiontable_id + " "
        sql_query += "(" + ','.join(map(str, columns)) + ") "
        sql_query += "VALUES (" + ','.join(map(str, values)) + ")"
        logging.info("Inserting row in fusiontable")
        request = self.service.query().sql(sql=sql_query)
        res = request.execute()
        logging.info("Row inserted!")
        return res

    def _makesets(self, columns, values):
        setstr = "SET "
        for i in range(len(columns)):
            setstr += str(columns[i]) + "=" + str(values[i]) + ", "
        return setstr[:-2]

    def _updateRow(self, fusiontable_id, ROWIDvalue, columns, values):
        """
            Update a row
        """
        parsedROWIDvalue = self._parseValue(str, ROWIDvalue)
        sql_query = "UPDATE " + fusiontable_id + " "
        sql_query += self._makesets(columns, values) + " "
        sql_query += "WHERE ROWID" + "=" + parsedROWIDvalue
        logging.info("Updating a row in fusiontable")
        request = self.service.query().sql(sql=sql_query)
        res = request.execute()
        logging.info("Row updated!")
        return res

    def _parseValue(self, odbc_type, val):
        """
            Parse python types for fusiontables
        """
        if val is None:
            return "''"
        if odbc_type == str:
            return "'" + val + "'"
        if odbc_type == bool:
            return int(val)
        if odbc_type == int:
            return val

    def getColumnsValuesParsed(self, header, row):
        values = []
        columns = []
        for c in header:
            values.append(self._parseValue(c[1], row[c[0]]))
            columns.append(c[0])
        return values, columns

    def insertRowDict(self, fusiontable_id, header, row):
        """
            Insert a row formmated as python dict
        """
        values, columns = self.getColumnsValuesParsed(header, row)
        return self._insertRow(fusiontable_id, columns, values)

    def getROWIDs(self, fusiontable_id, pkname):
        sql_query = "SELECT " + pkname + ", ROWID FROM " + fusiontable_id
        request = self.service.query().sql(sql=sql_query)
        fusiontable = request.execute()
        return fusiontable["rows"]

    def updateRowDict(self, fusiontable_id, header, ROWIDvalue, row):
        """
            Update a row formmated as python dict
        """
        values, columns = self.getColumnsValuesParsed(header, row)
        e = self._updateRow(fusiontable_id, ROWIDvalue, columns, values)
        return e
