# OURO_TRADER:  Buys and sells stocks based on signals from pathfinder
# Written by Dave Andrus on May 17, 2020
# Copyright 2020 Agile Data Guru
# https://github.com/AgileDataGuru/Ouro

# Required modules
import ouro_lib as ol
import json                             # for manipulating array data
import os                               # for basic OS functions
import pandas as pd                     # in-memory database capabilities
import argparse
from progress.bar import Bar
import datetime                         # used for stock timestamps
import alpaca_trade_api as tradeapi     # required for interaction with Alpaca
import csv
import logging

# Get Quorum path from environment
quorumroot = os.environ.get("OURO_QUORUM", "C:\\TEMP")
actionpath = quorumroot + '\\broker-actions.json'
buyskippath = quorumroot + '\\broker-buyskip.json'
statuspath = quorumroot + '\\broker-status.csv'
logpath = quorumroot + '\\trader.log'
installpath = os.environ.get("OURO_INSTALL", "D:\\OneDrive\\Dev\\Python\\Oura")

# Setup Logging
logging.basicConfig(
    filename=logpath,
    filemode='a',
    format='%(asctime)s %(name)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=os.environ.get("LOGLEVEL", "INFO"))

logging.info('OURO-TRADER logging enabled.')

# initialize files
try:
    logging.debug('Initializing trader files.')
    with open (statuspath, 'w', newline='\n', encoding='utf-8') as outfile:
        outfile.write('')
    with open (buyskippath, 'w', newline='\n', encoding='utf-8') as outfile:
        outfile.write('')
except Exception:
    logging.error('Could not initialize files', exc_info=True)
    quit()

# setup command line
parser = argparse.ArgumentParser(description="OURO-HISTORY:  Daily stock data ingestion.")
parser.add_argument("--test", action="store_true", default=False, help="Script runs in test mode.  FALSE (Default) = ignore if the market is closed; TRUE = only run while the market is open")
cmdline = parser.parse_args()
logging.info('Command line arguement; test mode is ' + str(cmdline.test))

# define time-in-force rule
if cmdline.test:
    inforce = 'gtc' # I don't want orders to error-out when testing
    logging.debug('Inforce set to GTC')
else:
    inforce = 'day' # when trading for real, I should use instant-or-cancel (IOC) orders to avoid waiting
    logging.debug('Inforce set to DAY')

# Initialize the Alpaca API
alpaca = tradeapi.REST()

# Read the buy and sell strategies
strategies = pd.read_csv(installpath + '\\buy_strategies.csv')

# build simple index between family and average return percentage
familyreturns = {}
try:
    logging.debug('Building strategy families and average return percentages')
    for x in strategies['Family'].keys():
        familyreturns[strategies.at[x, 'Family']] = strategies.at[x, 'AvgPctRtn']
except Exception:
    logging.error('Could not build strategies', exc_info=True)


# Set maximum risk ratio to 0.5% of the account
# This ratio cannot be exceeded on a single trade
maxriskratio = .004

# Initialize the stock lists
boughtlist = []
skiplist = []
status = {}

# Wait for the market to open unless it's a test
while not ol.IsOpen() and not cmdline.test:
    logging.info('Market is closed; waiting for 1 minute.')
    ol.WaitForMinute()

# Initialize MarketOpen
marketopen = ol.IsOpen()
eod = ol.IsEOD()

# This is to work around an odd problem with Alpaca and the way it reports cash / buying power
account = ol.GetAccount()
cash = (float(account.buying_power) / (float(account.multiplier)))-25001 # minimum amount for day trading
tradecapital = cash / 10

while (marketopen and not eod) or cmdline.test is True:
    marketopen = ol.IsOpen()
    eod = ol.IsEOD()

    # get ticker actions
    with open(actionpath, 'r', encoding='utf-8') as infile:
        inboundactions = json.load(infile)

    #setup a progress bar
    starttime = datetime.datetime.now().strftime('%H:%M:%S')
    logtime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    prgbar = Bar('  Stocks ' + starttime + ': ', max=len(inboundactions))

    ordercount=ol.GetOrderCount()
    # loop through the stocks
    for stock in inboundactions:
        if stock not in boughtlist and stock not in skiplist:
            # how much is the stock
            stockprice = float(inboundactions[stock].get('price'))
            recenthigh = float(inboundactions[stock].get('recenthigh'))
            recentlow = float(inboundactions[stock].get('recentlow'))

            # get basic account info; I need account.cash for calculations
            # account = ol.GetAccount()
            # cash = (float(account.buying_power) / (float(account.multiplier)))-25001 # minimum amount for day trading

            #set max trade risk
            maxriskamt = cash * maxriskratio
            traderiskamt = cash * maxriskratio

            # how much capital should I use on this trade?
            if ordercount < 10:
                # tradecapital = cash / float(10-ordercount)
                logging.debug('Orders are < 10; trade capital set to ' + str(tradecapital))
            else:
                # tradecapital = 0
                logging.debug('Orders count is > 10; trade capital set to 0')

            # how many shares should I buy
            ordershares = int(tradecapital/float(inboundactions[stock].get('price')))

            # reset pricing
            floorprice = 0
            ceilingprice = 0
            buylimit = 0
            traderiskpct = 0
            familyret = 0

            if stock not in boughtlist and ordercount < 10:
                # reset the skip reason
                skipreason = 'Unknown'

                # get the family return percent
                family = inboundactions[stock].get('strategyfamily')
                familyret = float(familyreturns[family])

                # set per-share floor price for bracket order
                # floorprice = stockprice - float(traderiskamt/ordershares)
                floorpct = familyret * .5
                floorprice = stockprice * (1-floorpct)

                # set the ceiling price for the bracket order
                ceilingprice = stockprice * (1 + float(familyreturns[family]))

                # Pricing reality check -- are the prices achievable in the recent past?
                if ceilingprice > recenthigh:
                    ceilingprice = recenthigh - 0.05  # $0.05 under the recent high
                if floorprice > recentlow:
                    skipreason = 'Proposed stop-loss price already hit today'
                    ordershares = 0

                # Adjust the floor price if this looks like a bad pick
                if (stockprice * ordershares * floorpct) > maxriskamt:
                    floorprice = stockprice - ((ceilingprice-stockprice) * .4) # I never want to break even on risk

                # Calculate the amount risked on this trade
                traderiskamt = (stockprice - floorprice) * ordershares
                traderiskpct = (stockprice - floorprice) / stockprice

                # set the buy limit to 5% of the potential profit
                buylimit = ((ceilingprice - stockprice) * .05) + stockprice

                # Calculate the trade return
                traderet = (ceilingprice - buylimit) / buylimit

                # Are we planning on making more than we risk?
                if traderet-.005 <= traderiskpct:
                    # This is a bad trade
                    ordershares = 0
                    skipreason = 'Risk outweighs reward'

                # place the order
                if ordershares > 0:
                    try:
                        logging.debug('Placing a bracket order for' + stock)
                        alpaca.submit_order(
                            side='buy',
                            symbol=stock,
                            type='limit',
                            limit_price=buylimit,
                            qty=ordershares,
                            time_in_force='day', # bracket order must be 'day' or 'gtc'
                            order_class='bracket',
                            take_profit={
                                'limit_price': ceilingprice
                            },
                            stop_loss={
                                'stop_price': floorprice
                            }

                        )
                        # Add this to the stocks already bought
                        # Note:  Only add this to the bought list if the placing the order was successful
                        #        This allows the stock to be re-tried if the price falls below the stop
                        #        point before the buy order can be filled.
                        boughtlist.append(stock)
                        status[stock] = {
                            'DateTime': logtime,
                            'Ticker': stock,
                            'Cash': cash,
                            'TradeCapital': tradecapital,
                            'BuyPrice': stockprice,
                            'BuyLimit': buylimit,
                            'MaxRiskAmt': maxriskamt,
                            'TradeRiskAmt': traderiskamt,
                            'RiskPct': traderiskpct,
                            'FamilyReturnPct': familyret,
                            'TradeReturnPct' : traderet,
                            'OrderShares': ordershares,
                            'RecentHigh': recenthigh,
                            'RecentLow':  recentlow,
                            'FloorPrice': floorprice,
                            'CeilingPrice': ceilingprice,
                            'Decision': 'buy',
                            'Reason': family
                        }
                    except Exception as ex:
                        logging.error('Could not submit buy order', exc_info=True)
                        logging.info('Skipping ' + stock + ' because buy order failed.')
                        if stock not in skiplist:
                            # add this to the skip list -- the timing just wasn't right
                            skiplist.append(stock)
                            status[stock] = {
                                'DateTime': logtime,
                                'Ticker': stock,
                                'Cash': cash,
                                'TradeCapital': tradecapital,
                                'BuyPrice': stockprice,
                                'BuyLimit': buylimit,
                                'MaxRiskAmt': maxriskamt,
                                'TradeRiskAmt': traderiskamt,
                                'RiskPct': traderiskpct,
                                'FamilyReturnPct': familyret,
                                'TradeReturnPct': traderet,
                                'OrderShares': ordershares,
                                'RecentHigh': recenthigh,
                                'RecentLow': recentlow,
                                'FloorPrice': floorprice,
                                'CeilingPrice': ceilingprice,
                                'Decision': 'skip',
                                'Reason': skipreason + ' - buy order failed.'
                            }
                else:
                    # define skipping reasons if not previously defined
                    if ordershares == 0 and skipreason == 'Unknown':
                        skipreason = 'Stock is too expensive or unable to buy shares.'
                    if stock in boughtlist and skipreason == 'Unknown':
                        skipreason = 'Stock in bought list.'
                    if ordercount >= 10 and skipreason == 'Unknown':
                        skipreason = 'Too many existing positions'

                    logging.info('Skipping ' + stock)
                    if stock not in skiplist:
                        # add this to the skip list -- the timing just wasn't right
                        skiplist.append(stock)
                        status[stock] = {
                            'DateTime': logtime,
                            'Ticker': stock,
                            'Cash': cash,
                            'TradeCapital': tradecapital,
                            'BuyPrice': stockprice,
                            'BuyLimit': buylimit,
                            'MaxRiskAmt': maxriskamt,
                            'TradeRiskAmt': traderiskamt,
                            'RiskPct': traderiskpct,
                            'FamilyReturnPct': familyret,
                            'TradeReturnPct' : traderet,
                            'OrderShares': ordershares,
                            'RecentHigh': recenthigh,
                            'RecentLow':  recentlow,
                            'FloorPrice': floorprice,
                            'CeilingPrice': ceilingprice,
                            'Decision': 'skip',
                            'Reason': skipreason
                        }
            # Update the order count after submitting the order
            ordercount = ol.GetOrderCount()

        #advance the progress bar
        prgbar.next()

    # finish the progress bar
    prgbar.finish()

    # write the bought and skip lists
    try:
        logging.debug('Writing bought and skip list.')
        with open (buyskippath, 'w', newline='\n', encoding='utf-8') as outfile:
            tmp = {
                'buy': boughtlist,
                'skip': skiplist
            }
            tmp = json.dumps(status, indent=4)
            outfile.write(tmp)
    except Exception:
        try:
            logging.error('Could not write buy and skip list', exc_info=True)
        except:
            print('Could not write to log file')

    # update broker status
    try:
        logging.debug('Writing broker status')
        with open (statuspath, 'w', newline='\n', encoding='utf-8') as outfile:
            fieldnames = ['DateTime', 'Ticker', 'Cash', 'TradeCapital', 'BuyPrice', 'BuyLimit', 'MaxRiskAmt',
                          'TradeRiskAmt', 'RiskPct', 'FamilyReturnPct', 'TradeReturnPct', 'OrderShares', 'RecentHigh',
                          'RecentLow','FloorPrice', 'CeilingPrice', 'Decision', 'Reason']
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            # write the header
            writer.writeheader()
            #writer.writerow(['datetime', 'ticker', 'cash', 'TradeRiskAmt', 'TradeCapital', 'OrderShares', 'FloorPrice', 'CeilingPrice', 'Decision'])
            for x in status:
                writer.writerow(status[x])
    except Exception:
        try:
            logging.error('Could not write broker status', exc_info=True)
        except:
            print('Could not to log file.')


    # wait until the next minute before checking again
    ol.WaitForMinute()

# It's the end of the day; cancel orders and quit
alpaca.cancel_all_orders()
alpaca.close_all_positions()





