import json
from fusiontablerest import FusionTableREST

def printRows(rows):
    for r in rows:
        print r

def showColumnNames(columns):
    for colname in columns:
        print colname

def toText(s):
    return "'"+s+"'"

def main():
    p12filename = "Fusionv2-526b826562a0"
    with open("config.json") as data_file:
        data = json.load(data_file)

    rest = FusionTableREST(
        data["client_email"],
        p12filename,
        data["private_key_password"]
    )
    test_fusiontable_id = data["fusiontables_ids"][0]["id"]

    # rows = rest.getRows(test_fusiontable_id)
    # columns = rest.getColumns(test_fusiontable_id)
    #
    # printRows(rows)
    # showColumnNames(columns)

    # e = rest.insertRow(
    #     test_fusiontable_id,
    #     ["Text", "Number", "Date"],
    #     [toText("Facundo"), "3", toText("2016-09-18")]
    # )
    #
    # print e
    r = rest.importCSV(test_fusiontable_id, "data")

if __name__ == '__main__':
    main()
