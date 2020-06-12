# DAILY_HISTORY V2:  Gets minute and day historical data from Alpaca and stores it in project database
# V1 Written by Dave Andrus and Dwight Brookland on April 13, 2020
# V2 Written by Dave Andrus on June 8, 2020
# Copyright 2020 Agile Data Guru
# https://github.com/AgileDataGuru/Ouro

# Required modules
import os                               # for basic OS functions
import logging                          # for application logging
import csv                              # for reading static data
import yfinance as yf                   # for stock list
import alpaca_trade_api as tradeapi     # required for trading
import datetime                         # used for stock timestamps
from datetime import timedelta
import json
from dateutil.parser import parse       # used to create date/time objects from stringsec
import time
import argparse
import ouro_lib as ol
from progress.bar import Bar            # for progress bars

# Set required paths
quorumroot = os.environ.get("OURO_QUORUM", "C:\\TEMP")
logpath = quorumroot + '\\history.log'

# Setup Logging
logging.basicConfig(
    filename=logpath,
    filemode='a',
    format='%(asctime)s %(name)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=os.environ.get("LOGLEVEL", "INFO"))
logging.info('OURO-HISTORY logging enabled.')

# Setup command line arguements
parser = argparse.ArgumentParser(description="OURO-HISTORY:  Daily stock data ingestion.")
parser.add_argument("--test", action="store_true", default=False, help="Script runs in test mode.  FALSE (Default) = update the entire universe of stock; TRUE = update a small subset of stocks to make testing quicker")
parser.add_argument("--dd", action="store_true", default=False, help="Daily Data.  FALSE (Default) = Skip daily data; TRUE = Get daily stock data for the unversive of stocks.")
parser.add_argument("--md", action="store_true", default=False, help="Minute Data.  FALSE (Default) = Skip minute-by-minute data; TRUE = Get the minute-by-minute data for the universe of stocks.")
parser.add_argument("--id", action="store_true", default=False, help="Indicator Data.  FALSE (Default) = Skip calculating indicators; TRUE = Calculate the daily and minute indicators for stocks with sufficient data.")
parser.add_argument("--recalc", action="store_true", default=False, help="Indicator Data.  FALSE (Default) = Find the best date to start calculating indicators; TRUE = Ignore existing data and forcibly recalculate all indicators for all stocks.")
cmdline = parser.parse_args()

# Set the paramaters correctly
if not cmdline.id and cmdline.recalc:
    logging.warning('Cannot skip indicators and force recalculation. --ID must be true to force a recalculation.')
    cmdline.id = True

# Setup the API
api = tradeapi.REST()
logging.info('Trade API configured.')
logging.info('Test mode is:  ' + str(cmdline.test))
logging.info('Getting daily data:  ' + str(cmdline.dd))
logging.info('Getting minute data:  ' + str(cmdline.md))
logging.info('Calculating indicators:  ' + str(cmdline.id))
logging.info('Force indicator recalculation:  ' + str(cmdline.recalc))

# Calculate universe of stocks
if not cmdline.test:
    # If it's not a test, get the list of assets from the data service
    assets = api.list_assets()
    slist = []
    for x in assets:
        if x.exchange == 'NASDAQ' or x.exchange=='NYSE':
            if x.tradable is True and x.status == 'active':
                slist.append(x.symbol)
                logging.debug('Adding ' + x.symbol + ' to the universe of stocks.')
            else:
                logging.debug('Skipping ' + x.symbol + ' because it is not tradable or active.')
        else:
            logging.debug('Skipping ' + x.symbol + ' because it is not in NYSE or NASDAQ.')
else:
    # Test with a small diverse set of stocks
    slist = ['CVS', 'DRI', 'EVR','FDEF', 'IBM', 'MPW', 'PPBI', 'PPL', 'PXD', 'QIWI', 'RL', 'TX', 'VZ']
logging.info('Universe of stocks created; ' + str(len(slist)) + ' stocks in the list.')

# Initialize SQL connection
sqlc = ol.sqldbcursor()

# configure common dates
today = datetime.datetime.utcnow()
today_str = today.strftime('%Y-%m-%d')
yesterday = today - datetime.timedelta(days=1)
earliest = today - datetime.timedelta(days=180)

# Create a progress bar
prgbar = Bar('  Stocks', max=len(slist)+1, suffix='%(index)d/%(max)d %(percent).1f%% - %(eta)ds  ')
prgbar.next()

# Are we going to skip minute data
skipmindata = True  # By default, assume we're skipping minute data

for x in slist:
    ###
    # DAY TIMEFRAME ACTIONS
    ###

    if cmdline.dd:
        # Calculate the last day-date for the stock
        query = "SELECT MAX(tradedate) FROM stockdata..ohlcv_day WHERE ticker = '" + x + "';"
        startdate = today
        try:
            startdate = earliest
            dts = ol.qrysqldb(sqlc, query).fetchone()[0]
            if dts is not None:
                try:
                    # sometimes dts is a string
                    startdate = parse(dts) + timedelta(days=1)
                except:
                    # sometimes dts is a date
                    startdate = dts + timedelta(days=1)
        except Exception as ex:
            logging.error('Setting day interval start date to earliest date. ', exc_info=True)
            startdate = earliest

        # If the last date was in the past, get new data; otherwise, skip it
        startdate_str = startdate.strftime('%Y-%m-%d')
        if startdate < today:
            try:
                logging.info('Getting data between ' + startdate_str + ' and ' + today_str + ' for ' + x)
                data = ol.GetOHLCV(ticker=x, startdate=startdate_str, enddate=today_str, timeframe='1D')
            except Exception as ex:
                logging.warning ('Data for ' + x + ' has problems; it is being skipped.', exc_info=True)

            if not data.empty:
                try:
                    logging.info('Writing day-interval data for ' + x)
                    ol.WriteOHLCV(data, timeframe='1D')
                    skipmindata = False # Minute data is only eligible if there is day data
                except Exception as ex:
                    logging.error('Could not write day-interval data for' + x, exc_info=True)
            else:
                logging.info('No day-interval data for ' + x + ' on ' + startdate_str + '; not writing anything.')
                skipmindata = True  # This confirms that we need to skip minute data
    else:
        logging.info('Skipping daily data for ' + x + ' by request.')

    ###
    # MINUTE TIMEFRAME ACTIONS
    ###
    if cmdline.md:
        # For minute data, I don't want to go back that far
        earliest = today - datetime.timedelta(days=42)

        # Calculate the last day-date for the stock
        query = "SELECT MAX(tradedatetime) FROM stockdata..ohlcv_minute WHERE ticker = '" + x + "';"
        startdate = today
        try:
            startdate = earliest
            dts = ol.qrysqldb(sqlc, query).fetchone()[0]
            if dts is not None:
                try:
                    # sometimes dts is a string
                    startdate = parse(dts) + timedelta(days=1)
                except:
                    # sometimes dts is a date
                    startdate = dts + timedelta(days=1)
        except Exception as ex:
            logging.error('Setting minute-interval start date to earliest date. ', exc_info=True)
            startdate = earliest

        # Loop through each day until we get caught up
        while startdate < today and not skipmindata:
            # Set the start date
            startdate_str = startdate.strftime('%Y-%m-%d')

            # Get the data
            try:
                logging.info('Getting data between ' + startdate_str + ' and ' + today_str + ' for ' + x)
                data = ol.GetOHLCV(ticker=x, startdate=startdate_str, enddate=today_str, timeframe='1Min')
            except Exception as ex:
                logging.warning ('Minute-data for ' + x + ' has problems; it is being skipped.', exc_info=True)

            # Write the data
            if not data.empty:
                try:
                    logging.info('Writing day-interval data for ' + x)
                    ol.WriteOHLCV(data, timeframe='1Min')
                except Exception as ex:
                    logging.error('Could not write minute-interval data for' + x, exc_info=True)
            else:
                logging.info('No minute-interval data for ' + x + ' on ' + startdate_str + '; not writing anything.')

            # move onto the next day
            startdate = startdate + timedelta(days=1)

    else:
        logging.info('Skipping minute data for ' + x + ' by request.')

    ###
    # TECHNICAL INDICATORS
    ###





    prgbar.next()

prgbar.finish()



