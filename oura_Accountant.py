# OURO_ACCOUNTANT:  Translates Alpaca transactions into a profit-loss type journal
# for the purposes of filing IRS Form 4797.
# Written by Dave Andrus on April 30, 2020
# Copyright 2020 Agile Data Guru
# https://github.com/AgileDataGuru/Ouro

# Required modules
import os                               # for basic OS functions
import logging                          # for application logging
import alpaca_trade_api as tradeapi     # required for interaction with Alpaca
import ouro_lib as ol                   # for shared Ouro functions
import pyodbc                           # for connections to SQL Server
import argparse
import datetime
from datetime import timedelta
from dateutil.parser import parse       # used to create date/time objects from strings

# import datetime                         # used for stock timestamps
# import json                             # for manipulating array data
# import uuid
# from azure.cosmos import exceptions, CosmosClient, PartitionKey
# from dateutil.parser import parse       # used to create date/time objects from stringsec
# import time                             # for manipulating time data
# import pandas as pd                     # in-memory database capabilities
# import talib as ta                      # lib to calcualted technical indicators
# from pandas.io.json import json_normalize
# from progress.bar import Bar
# import threading
# import math
# import csv

def CheckLedger(cursor, tradedate, ticker, assetclass):
    # checks if the ticker/tradedate pair is in the accountant_details table
    # if is is not, it creates it so the main program can just update the table

    retval = False

    # Check if the key pair exists
    try:
        qry = "SELECT count(*) FROM accountant_details WHERE ticker ='" + ticker + "' AND tradedate = '" + tradedate + "';"
        cursor.execute(qry)
        t = cursor.fetchone()[0]
    except:
        logging.debug ('Pair not found; create it.')

    if t == 0:
        # nothing exists
        try:
            qry = "INSERT INTO accountant_details (ticker, tradedate, assetclass) VALUES ('" + ticker + "', '" + tradedate + "', '" + assetclass + "')"
            cursor.execute(qry)
            retval = True
        except Exception as ex:
            logging.error('Could not create ticker / tradedate pair in stockdata.accountant_details')
    else:
        retval = True
    return retval

# Form requirements:
#   10a = Description of property
#   10b = Date acquired (mo/day/year)
#   10c = Date sold (mo/day/year)
#   10d = Gross sales price
#   10e = Depreciation
#   10f = Cost or other basis plus improvements and expense of sale
#   10g = Gain or loss

# Setup Logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
logging.info('OURO-ACCOUNTANT logging enabled.')

# setup command line
parser = argparse.ArgumentParser(description="OURO-HISTORY:  Daily stock data ingestion.")
parser.add_argument("--user", default='ouro_service', help="The user name to log into SQL Server with.  (Default) = Ouro_Service")
parser.add_argument("--pwd", required=True, help="The password to use when logging into the Ouro database")
cmdline = parser.parse_args()

sqluser = cmdline.user
sqlpwd = cmdline.pwd

# Get Quorum path from environment
quorumroot = os.environ.get("OURO_QUORUM", "C:\\TEMP")

# initialize quorum files
logging.info('Quorum path set to ' + quorumroot)

# Initialize the Alpaca API
alpaca = tradeapi.REST()

# Initialize the SQL Server connection
pyodbc.autocommit = True
sqlsvr = pyodbc.connect('DSN=Ouro;UID='+sqluser+';PWD='+sqlpwd, autocommit=True)
cursor = sqlsvr.cursor()

# Set date range
today = datetime.datetime.now()
qry = "SELECT max(tradedate) FROM stockdata..accountant_details"
cursor.execute(qry)
try:
    lastdate = parse(cursor.fetchone()[0])
except:
    lastdate = today
daterange = today - lastdate

# loop through the date range and assemble the orders
for day in range(0, daterange.days + 1):
    sdt = lastdate + timedelta(days=day)
    data = ol.GetOrders(status='closed', startdate=sdt.strftime('%Y-%m-%d'))
    logging.info ('Getting orders for ' + sdt.strftime('%Y-%m-%d'))

    for x in data:

        if x.limit_price is None:
            sel = 0
        else:
            sel = x.limit_price

        if x.stop_price is None:
            stl = 0
        else:
            stl = x.stop_price

        cl = CheckLedger(cursor, x.created_at.strftime('%Y-%m-%d'), x.symbol, x.asset_class)
        if cl and x.side == 'buy' and int(x.filled_qty) > 0:
            # we bought something

            qry = "UPDATE stockdata..accountant_details SET " \
                  "buyid = '" + x.id + "', " \
                  "buyts = '" + x.filled_at.strftime('%Y-%m-%d %H:%M:%S') + "', " \
                  "buylimit = " + str(sel) + ", " \
                  "buystop = " + str(stl) + ", " \
                  "buyqty = " + str(x.filled_qty) + ", " \
                  "buyprice = " + str(x.filled_avg_price) + ", " \
                  "buyfilledprice = " + str(x.filled_avg_price) + ", " \
                  "tax10a_desc = '" + str(x.filled_qty) + " shares of " + x.symbol + "', " \
                  "tax10b_acquired = '" + x.filled_at.strftime('%Y-%m-%d') + "', " \
                  "tax10e_depreciation = 0, " \
                  "tax10f_grosscost = " + str(float(x.filled_avg_price) * int(x.filled_qty)) + " " \
                  "WHERE ticker = '" + x.symbol + "' AND tradedate = '" + x.created_at.strftime('%Y-%m-%d') + "';"
            cursor.execute(qry)

        if cl and x.side == 'sell' and int(x.filled_qty) > 0:
            # we sold something

            qry = "UPDATE stockdata..accountant_details SET " \
                  "sellid = '" + x.id + "', " \
                  "sellts = '" + x.filled_at.strftime('%Y-%m-%d %H:%M:%S') + "', " \
                  "selllimit = " + str(sel) + ", " \
                  "sellstop = " + str(stl) + ", " \
                  "sellqty = " + str(x.filled_qty) + ", " \
                  "sellfilledprice = " + str(x.filled_avg_price) + ", " \
                  "tax10a_desc = '" + str(x.filled_qty) + " shares of " + x.symbol + "', " \
                  "tax10c_sold = '" + x.filled_at.strftime('%Y-%m-%d') + "', " \
                  "tax10d_grossprice = " + str(float(x.filled_avg_price) * int(x.filled_qty)) + " " \
                  "WHERE ticker = '" + x.symbol + "' AND tradedate = '" + x.created_at.strftime('%Y-%m-%d') + "';"
            cursor.execute(qry)

        # update the gain loss
        qry = "UPDATE stockdata..accountant_details SET tax10g_gainloss = tax10d_grossprice - tax10f_grosscost " \
              "WHERE tax10g_gainloss IS NULL AND tax10d_grossprice IS NOT NULL AND tax10f_grosscost IS NOT NULL"
        cursor.execute(qry)
