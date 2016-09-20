import logging, sys
import json
import re
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
    logging.info("Setting up DB Connection")
    with open("dbconnection.json") as data_file:
        data = json.load(data_file)
    connection = SQLConnector(
        data["server"],
        data["uid"], data["pwd"],
        data["dbs"][0]["dbname"]
    )
    return connection

def setupFusionTable(p12filename):
    logging.info("Setting Up Fusion Table config")
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

def getFromSquareBrackets(s):
    return re.findall(r"\['?([A-Za-z0-9_]+)'?\]", s)

def auxparse(e):
    try:
        e = int(e)
    except:
        pass
    return e

def castInts(l):
    return list((map(auxparse, l)))

def parseRoots(dics):
    """
        Returns pos id for list.
        Because we have formmatted [{id:{dic}}, {id:{dic}}]
    """
    values = []
    for d in dics:
        values.append(castInts(getFromSquareBrackets(d)))
    return values

def makeAddDicts(table, pkname, npos):
    added = []
    for p in npos:
        pkvalue = table[p[0]].keys()[0]
        d = table[p[0]].values()[0]
        d.update({pkname:pkvalue})
        added.append(d)
    return added

def executeDiff(localdb, tablename, fusiondb, fusiondbID):
    logging.info("Running diff analyzer")
    localdb.runSQLQuery("SELECT * FROM " + tablename )
    localrows = localdb.getRows()
    fusionrows = fusiondb.getRows(fusiondbID)
    db_columns = [c[0] for c in localdb.getHeaderInfo()]
    ft_columns = fusiondb.getColumns(fusiondbID)
    if db_columns == ft_columns:
        logging.info("Columns match! Now analyzing...")
        fusiondrows = convertFTRowsToDict(
            localdb.getHeaderInfo(),
            ft_columns,
            fusionrows
        )
    pk = str(localdb.getPK(tablename))
    localTable = makeDictsWithID(localrows, pk)
    fusionTable = makeDictsWithID(fusiondrows, pk)
    ddiff = DeepDiff(fusionTable, localTable, verbose_level=2)
    try:
        added = ddiff['iterable_item_added']
        #updated = ddiff['values_changed']
        npos_added = parseRoots(added)
        #npos_updated = parseRoots(updated)
        return makeAddDicts(localTable, pk, npos_added)
    except:
        return []



def insertdiffAdd(fusiondb, fusiontable_id, headerinfo, dics):
    for d in dics:
        fusiondb.insertRowDict(
            fusiontable_id,
            headerinfo,
            d
        )
    logging.info("Insertion complete!")

def main():
    logging.basicConfig(stream = sys.stderr, level=logging.DEBUG)
    localdb = getConnection()
    p12filename = "Fusionv2-526b826562a0"
    fusiondb, data = setupFusionTable(p12filename)
    fusiontable_id = data["fusiontables_ids"][0]["id"]
    tablename = "testPersona";
    rows_added = executeDiff(
        localdb,
        tablename,
        fusiondb,
        fusiontable_id
    )
    if len(rows_added):
        logging.info("Differences found. Now inserting...")
        insertdiffAdd(
            fusiondb,
            fusiontable_id,
            localdb.getHeaderInfo(),
            rows_added
        )
    else:
        logging.info("Nothing to be done.")




if __name__ == '__main__':
    main()
