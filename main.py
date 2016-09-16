import json
from fusiontablerest import FusionTableREST
from sqlconnector import SQLConnector

def printRows(rows):
    for r in rows:
        print r

def showColumnNames(columns):
    for colname in columns:
        print colname

def toText(s):
    return "'"+s+"'"

def getConnection():
    with open("dbconnection.json") as data_file:
        data = json.load(data_file)
    connection = SQLConnector(
        data["server"],
        data["uid"], data["pwd"],
        data["dbs"][0]["dbname"]
    )
    return connection

def setupFusionTable(p12filename):
    with open("config.json") as data_file:
        data = json.load(data_file)

    rest = FusionTableREST(
        data["client_email"],
        p12filename,
        data["private_key_password"]
    )
    return restapi, data

def main():
    con = getConnection()
    con.runSQLQuery("SELECT * FROM KML_Mapa")
    rows = con.getRows()
    print rows[0].keys()
    # p12filename = "Fusionv2-526b826562a0"
    # restapi, data = setupFusionTable(p12filename)
    #
    # kml_fusiontable_id = data["fusiontables_ids"][1]["id"]
    #
    # e = restapi.insertRow(
    #     test_fusiontable_id,
    #     ["idKML", "CodKML", "Date"],
    #     [toText("Facundo"), "3", toText("2016-09-18")]
    # )

if __name__ == '__main__':
    main()
