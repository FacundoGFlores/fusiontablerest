#!/usr/bin/env python
"""
    Keep updated a fusiontable from a localdb.
    There are two config files:
        - config.json
        - dbconnection.json
    config.json has settings for fusiontables
    dbconnection.json has settings for localdb
    pypyodbc allow us to connect to SQL Server
    executeDiff() run queries and diff analyzer for
    getting differences between fusion table and local db,
    but it assumes there are no empty tables.
"""


import datetime
import json
import logging
import re
import sys
from ast import literal_eval

from deepdiff import DeepDiff

from fusiontablerest import FusionTableREST
from sqlconnector import SQLConnector

__author__ = "Flores, Facundo Gabriel"

__version__ = "0.1.1"
__maintainer__ = "Flores, Facundo Gabriel"
__email__ = "flores.facundogabriel@gmail.com"
__status__ = "Development"


def toText(s):
    return "'" + s + "'"


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


def parseRoots(str_diff):
    """
        Returns pos id for list.
        Because we have formmatted [{id:{dic}}, {id:{dic}}]
    """
    return [tuple(literal_eval(y)
                  for y in re.findall(r"\[('?\w+'?)\]", x)) for x in str_diff]


def makeAddDicts(table, pkname, npos):
    added = []
    for p in npos:
        pkvalue = table[p[0]].keys()[0]
        d = table[p[0]].values()[0]
        d.update({pkname: pkvalue})
        added.append(d)
    return added


def makeUpdatedDicts(table, pkname, npos):
    """
        table:
        pkname:
        npos: List of tuples of type (pos, pkvalue, colums_changed)
    """
    logging.info("Making rows for update...")
    updated = []
    for p in npos:
        pos = p[0]
        pkvalue = p[1]
        column_changed = p[2]
        d = list(table[pos].values())[0]
        d.update({pkname: pkvalue})
        updated.append(d)
    return updated


def executeDiff(localdb, tablename, fusiondb, fusiondbID):
    """
        Important: we are not checking empty tables.
    """
    logging.info("Running diff analyzer")
    pk = str(localdb.getPK(tablename))
    localdb.runSQLQuery("SELECT * FROM " + tablename)
    localrows = localdb.getRows()
    fusionrows = fusiondb.getRows(fusiondbID, pk)
    db_columns = [c[0] for c in localdb.getHeaderInfo()]
    ft_columns = fusiondb.getColumns(fusiondbID)
    if db_columns == ft_columns:
        logging.info("Columns match! Now analyzing...")
        fusiondrows = convertFTRowsToDict(
            localdb.getHeaderInfo(),
            ft_columns,
            fusionrows
        )
    localTable = makeDictsWithID(localrows, pk)
    fusionTable = makeDictsWithID(fusiondrows, pk)
    ddiff = DeepDiff(fusionTable, localTable, ignore_order=False)
    if 'iterable_item_added' in ddiff.keys():
        added = ddiff['iterable_item_added']
        npos_added = parseRoots(added)
        dics_added = makeAddDicts(localTable, pk, npos_added)
    else:
        dics_added = []
    if 'values_changed' in ddiff.keys():
        updated = ddiff['values_changed']
        npos_updated = parseRoots(updated)
        dics_updated = makeUpdatedDicts(localTable, pk, npos_updated)
    else:
        dics_updated = []
    return dics_added, dics_updated


def insertdiffAdd(fusiondb, fusiontable_id, headerinfo, dics):
    for d in dics:
        fusiondb.insertRowDict(
            fusiontable_id,
            headerinfo,
            d
        )
    logging.info("Insertion completed!")


def associateROWIDsWithPK(ftresultforROWIDs):
    return {int(key): value for key, value in ftresultforROWIDs}


def insertdiffUpdates(
    fusiondb, fusiontable_id, headerinfo, pk, dics,
):
    result = fusiondb.getROWIDs(fusiontable_id, pk)
    pksassociated = associateROWIDsWithPK(result)
    for d in dics:
        pkvalue = d[pk]
        ROWIDvalue = pksassociated[pkvalue]
        fusiondb.updateRowDict(
            fusiontable_id,
            headerinfo,
            ROWIDvalue,
            d
        )
    logging.info("Update completed!")


def cleanAndfill(localdb, fusiondb, fusiontable_id, tablename):
    fusiondb.cleanTable(fusiontable_id)
    localdb.runSQLQuery("SELECT * FROM " + tablename)
    localdb.writeCSV("out")


def main():
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    logging.propagate = False
    localdb = getConnection()
    p12filename = "Fusionv2-526b826562a0"
    fusiondb, data = setupFusionTable(p12filename)
    fusiontable_id = data["fusiontables_ids"][0]["id"]
    tablename = "testPersona"
    # cleanAndfill(localdb, fusiondb, fusiontable_id, tablename)
    rows_added, rows_updated = executeDiff(
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
    if len(rows_updated):
        logging.info("Differences found. Now updating...")
        insertdiffUpdates(
            fusiondb,
            fusiontable_id,
            localdb.getHeaderInfo(),
            str(localdb.getPK(tablename)),
            rows_updated
        )
    logger = logging.getLogger('main')
    nowdate = datetime.datetime.now()
    strdate = str(nowdate.day) + str(nowdate.month) + str(nowdate.year)
    strdate += str(nowdate.hour) + str(nowdate.minute)
    hdlr = logging.FileHandler('ft' + strdate + '.log')
    logger.addHandler(hdlr)
    logger.setLevel(logging.INFO)
    if len(rows_added) == 0 and len(rows_updated) == 0:
        logger.info("Fusion table already Updated!.")
    else:
        logger.info("Rows Added: " + str(len(rows_added)))
        logger.info("Rows Updated: " + str(len(rows_updated)))
        logger.info("Fusion table Updated!.")

if __name__ == '__main__':
    main()
