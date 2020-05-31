# DAILY_TICALC:  Calculates technical indicators
# Written by Dave Andrus and Dwight Brookland on April 13, 2020
# Copyright 2020 Agile Data Guru
# https://github.com/AgileDataGuru/Ouro

# Required modules
import os                               # for basic OS functions
import logging                          # for application logging
import datetime                         # used for stock timestamps
import json
import uuid
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from dateutil.parser import parse       # used to create date/time objects from stringsec
import time
import pandas as pd
import talib as ta
import ouro_lib as ol
import argparse

# Setup Logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "DEBUG"))
logging.info('OURO-TICALC logging enabled.')

parser = argparse.ArgumentParser(description="DAILY-TICALC:  Daily technical indicator calculator.")
parser.add_argument("--source", default='db', help="Whether to get data from Yahoo Finance or cached data in SQL Server.  (Default) = db; (Alternate) = yahoo")
cmdline = parser.parse_args()

dhistory = None
indicators = None

if cmdline.source == 'yahoo':
    # Initialize the Cosmos client
    dhistory = ol.cosdb('stockdata', 'daily', '/ticker')
    indicators = ol.cosdb('stockdata', 'daily_indicators', '/ticker')
if cmdline.source == 'db':
    dhistory = ol.sqldbcursor()
    indicators = ol.sqldbconn()

if dhistory == None or indicators == None:
    logging.error('No valid sources, please choose "db" or "yahoo" only.')
    quit()

# get list of stocks in the universe
if cmdline.source == 'yahoo':
    query = "select distinct value d.ticker from daily d"
    stocklist = ol.qrycosdb(dhistory, query)
else:
    query = "SELECT DISTINCT ticker FROM stockdata..ohlcv_day"
    t = ol.qrysqldb(dhistory, query)
    stocklist = t.fetchall()

# process the daily stocks
for stock in stocklist:

# Get all the daily data for the past 60 days for the given stock
# Note: 4.88 RU per 100 rows
    if cmdline.source == 'yahoo':
        query = "SELECT d.id, d.ticker, d.tradedate, d.high as h, d.low as l, d.open as o, d.adjclose as c, d.volume as v FROM daily d where d.ticker = '" + stock + "'"
        df=pd.DataFrame(ol.qrycosdb(dhistory, query))
    else:
        query = "SELECT ticker, tradedate, h, l, o, c, v FROM stockdata..ohlcv_day where ticker = '" + stock[0] + "'"
        df = pd.read_sql_query(query, indicators)

    if not df.empty:
        # Get the max date that indicator data exists for this stock
        # Note: 4.88 RU per 100 rows
        dt = parse('1971-01-01')
        dt_str = dt.strftime('%Y-%m-%d')
        if cmdline.source == 'yahoo':
            query = "SELECT VALUE max(d.tradedate) from daily d where d.ticker = '" + stock + "'"
            dt_list = ol.qrycosdb(indicators, query)
        else:
            query = "SELECT min(tradedate) FROM stockdata..ohlcv_day WHERE strategy_id is NULL and ticker = '" + stock[0] + "'"
            dt_list = ol.qrysqldb(dhistory, query)
            dt = parse(dt_list.fetchone()[0])

        # Calcualte the indicators
        df = ol.calcind(df)

        # Write them back to the db
        rc=0
        for x in json.loads(df.to_json(orient='records')):
            if parse(x['tradedate']) > dt:
                if cmdline.source == 'yahoo':
                    # write the data to cosmosdb
                    try:
                        indicators.create_item(body=x)
                        logging.debug('Created document for ' + x['ticker'] + ' on ' + x['tradedate'])
                        rc = rc + 1
                    except:
                        logging.debug('Could not create document for ' + x['ticker'] + ' on ' + x['tradedate'])
                else:
                    query = "UPDATE stockdata..ohlcv_day SET " \
                            "ADX14 = " + str(x.get('ADX14')) + ", " \
                            "ADXR14 = " + str(x.get('ADXR14')) + ", " \
                            "APO12 = " + str(x.get('APO12')) + ", " \
                            "AROONUP = " + str(x.get('AROONUP')) + ", " \
                            "AROONDN = " + str(x.get('AROONDN')) + ", " \
                            "BOP = " + str(x.get('BOP')) + ", " \
                            "CCI14 = " + str(x.get('CCI14')) + ", " \
                            "CMO14 = " + str(x.get('CMO14')) + ", " \
                            "DX14 = " + str(x.get('DX14')) + ", " \
                            "MACD = " + str(x.get('MACD')) + ", " \
                            "MACDSIG = " + str(x.get('MACDSIG')) + ", " \
                            "MACDHIST = " + str(x.get('MACDHIST')) + ", " \
                            "MOM10 = " + str(x.get('MOM10')) + ", " \
                            "PPO12 = " + str(x.get('PPO12')) + ", " \
                            "ROC10 = " + str(x.get('ROC10')) + ", " \
                            "RSI14 = " + str(x.get('RSI14')) + ", " \
                            "STOCHK = " + str(x.get('STOCHK')) + ", " \
                            "STOCHD = " + str(x.get('STOCHD')) + ", " \
                            "STOCHRSIK = " + str(x.get('STOCHRSIK')) + ", " \
                            "STOCHRSID = " + str(x.get('STOCHRSID')) + ", " \
                            "TRIX30 = " + str(x.get('TRIX30')) + ", " \
                            "ULTOSC = " + str(x.get('ULTOSC')) + ", " \
                            "BBUPPER = " + str(x.get('ULTOSC')) + ", " \
                            "BBMID = " + str(x.get('BBMID')) + ", " \
                            "BBLOWER = " + str(x.get('BBLOWER')) + ", " \
                            "EMA14 = " + str(x.get('EMA14')) + ", " \
                            "SMA14 = " + str(x.get('SMA14')) + ", " \
                            "AD = " + str(x.get('AD')) + ", " \
                            "ADOSC = " + str(x.get('ADOSC')) + ", " \
                            "OBV = " + str(x.get('OBV')) + ", " \
                            "DJI = " + str(x.get('DJI')) + ", " \
                            "ENG = " + str(x.get('ENG')) + ", " \
                            "HMR = " + str(x.get('HMR')) + ", " \
                            "HGM = " + str(x.get('HGM')) + ", " \
                            "PRC = " + str(x.get('PRC')) + ", " \
                            "DCC = " + str(x.get('DCC')) + ", " \
                            "MSR = " + str(x.get('MSR')) + ", " \
                            "ESR = " + str(x.get('ESR')) + ", " \
                            "KKR = " + str(x.get('KKR')) + ", " \
                            "SSR = " + str(x.get('SSR')) + ", " \
                            "IHM = " + str(x.get('IHM')) + ", " \
                            "TWS = " + str(x.get('TWS')) + ", " \
                            "TBC = " + str(x.get('TBC')) + ", " \
                            "STP = " + str(x.get('STP')) + ", " \
                            "ADXTREND = '" + str(x.get('ADXTREND')) + "', " \
                            "ADXRTREND = '" + str(x.get('ADXRTREND')) + "', " \
                            "AROONVOTE = '" + str(x.get('AROONVOTE')) + "', " \
                            "BOPVOTE = '" + str(x.get('BOPVOTE')) + "', " \
                            "CCIVOTE = '" + str(x.get('CCIVOTE')) + "', " \
                            "MACDVOTE = '" + str(x.get('MACDVOTE')) + "', " \
                            "PPOVOTE = '" + str(x.get('PPOVOTE')) + "', " \
                            "RSIVOTE = '" + str(x.get('RSIVOTE')) + "', " \
                            "STOCHVOTE = '" + str(x.get('STOCHVOTE')) + "', " \
                            "STOCHRSIVOTE = '" + str(x.get('STOCHRSIVOTE')) + "', " \
                            "TRIXVOTE = '" + str(x.get('TRIXVOTE')) + "', " \
                            "ADOSCVOTE = '" + str(x.get('ADOSCVOTE')) + "', " \
                            "STRATEGY_ID = '" + str(x.get('STRATEGY_ID')) + "'" \
                            "WHERE ticker = '" + str(x.get('ticker')) + "' AND " \
                            "tradedate = '" + str(x.get('tradedate')) + "'"
                    # Replace Python None with SQL Null
                    query = query.replace('None', 'NULL')

                    # update the data
                    ol.qrysqldb(dhistory, query)
                    rc = rc + 1

        if cmdline.source == 'yahoo':
            logging.info('Wrote ' + str(rc) + ' records for ' + stock)
        else:
            logging.info('Wrote ' + str(rc) + ' records for ' + stock[0])
