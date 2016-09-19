import json
from fusiontablerest import FusionTableREST
from sqlconnector import SQLConnector
from deepdiff import DeepDiff

def printRows(rows):
    for r in rows:
        print r

def showColumnNames(columns):
    for colname in columns:
        print colname

def toText(s):
    return "'"+s+"'"

def getConnection():
    print "Setting up DB Connection"
    with open("dbconnection.json") as data_file:
        data = json.load(data_file)
    connection = SQLConnector(
        data["server"],
        data["uid"], data["pwd"],
        data["dbs"][0]["dbname"]
    )
    return connection

def setupFusionTable(p12filename):
    print "Setting Up Fusion Table config"
    with open("config.json") as data_file:
        data = json.load(data_file)

    rest = FusionTableREST(
        data["client_email"],
        p12filename,
        data["private_key_password"]
    )
    return rest, data

def parseVal(coltype, val):
    try:
        if coltype == str:
            return val
        if coltype == int:
            return int(val)
        if coltype == bool:
            return bool(val)
    except ValueError:
        return None

def makeDictFromFTRow(headerinfo, ftcolumns, ftrow):
    rowDict = {}
    for i in range(len(ftcolumns)):
        colname = headerinfo[i][0]
        coltype = headerinfo[i][1]
        val = parseVal(coltype, ftrow[i])
        if val is None:
            return None
        else:
            rowDict[colname] = parseVal(coltype, ftrow[i])
    return rowDict

def convertFTRowsToDict(headerinfo, columns, rows):
    rows_added = []
    for row in rows:
        d = makeDictFromFTRow(
            headerinfo,
            columns,
            row
        )
        if d is not None:
            rows_added.append(d)
    return rows_added


def new_dict(old_dict, pk):
    n = old_dict.copy()
    pkvalue = n[pk]
    pkpair = n.pop(pk, None)
    return {pkvalue: n}

def makeDictsWithID(dics, pk):
    reestructured = []
    for d in dics:
        reestructured.append(new_dict(d, pk))
    return reestructured

def executeDiff(localdb, fusiondb, fusiondbID):
    print "Running diff analyzer"
    tablename = "KML_Mapa";
    localdb.runSQLQuery("SELECT * FROM " + tablename )
    localrows = localdb.getRows()
    fusionrows = fusiondb.getRows(fusiondbID)
    db_columns = [c[0] for c in localdb.getHeaderInfo()]
    ft_columns = fusiondb.getColumns(fusiondbID)
    if db_columns == ft_columns:
        print "Columns match! Now analyzing"
        fusiondrows = convertFTRowsToDict(
            localdb.getHeaderInfo(),
            ft_columns,
            fusionrows
        )
    pk = localdb.getPK("KML_Mapa")
    localTable = makeDictsWithID(localrows, pk)
    fusionTable = makeDictsWithID(fusiondrows, pk)
    print localTable[len(localTable)-1]
    print fusionTable[len(fusionTable)-1]
    ddiff = DeepDiff(localTable, fusionTable, ignore_order=False)
    print (ddiff)

def main():
    localdb = getConnection()
    p12filename = "Fusionv2-526b826562a0"
    fusiondb, data = setupFusionTable(p12filename)
    fusiontable_id = data["fusiontables_ids"][1]["id"]
    executeDiff(localdb, fusiondb, fusiontable_id)



if __name__ == '__main__':
    main()
