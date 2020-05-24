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


print(-.5 > -.6)
quit()

# Get Quorum path from environment
quorumroot = os.environ.get("OURO_QUORUM", "C:\\TEMP")
actionpath = quorumroot + '\\broker-actions.json'
buyskippath = quorumroot + '\\broker-buyskip.json'
statuspath = quorumroot + '\\broker-status.csv'

# initialize files
with open (statuspath, 'w', newline='\n', encoding='utf-8') as outfile:
    outfile.write('')
with open (buyskippath, 'w', newline='\n', encoding='utf-8') as outfile:
    outfile.write('')

# setup command line
parser = argparse.ArgumentParser(description="OURO-HISTORY:  Daily stock data ingestion.")
parser.add_argument("--test", action="store_true", default=False, help="Script runs in test mode.  FALSE (Default) = ignore if the market is closed; TRUE = only run while the market is open")
cmdline = parser.parse_args()

# define time-in-force rule
if cmdline.test:
    inforce = 'gtc' # I don't want orders to error-out when testing
else:
    inforce = 'ioc' # when trading for real, I should use instant-or-cancel orders to avoid waiting

# Initialize the Alpaca API
alpaca = tradeapi.REST()

# Read the buy and sell strategies
strategies = pd.read_csv('D:\\OneDrive\\Dev\\Python\\Oura\\buy_strategies.csv')

# build simple index between family and average return percentage
familyreturns = {}
for x in strategies['Family'].keys():
    familyreturns[strategies.at[x, 'Family']] = strategies.at[x, 'AvgPctRtn']

# Set maximum risk ratio to 0.5% of the account
# This ratio cannot be exceeded on a single trade
maxriskratio = .005

# Initialize the stock lists
boughtlist = []
skiplist = []
status = {}

# Wait for the market to open unless it's a test
while not ol.IsOpen() and not cmdline.test:
    ol.WaitForMinute()

# Initialize MarketOpen
marketopen = ol.IsOpen()

while (marketopen and not ol.IsEOD) or cmdline.test is True:
    marketopen = ol.IsOpen()

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

            # get basic account info; I need account.cash for calculations
            account = ol.GetAccount()
            cash = (float(account.buying_power) / (float(account.multiplier)))-25001 # minimum amount for day trading

            #set max trade risk
            traderiskamt = cash * maxriskratio

            # how much capital should I use on this trade?
            if ordercount < 10:
                tradecapital = cash / float(10-ordercount)
            else:
                tradecapital = 0

            # how many shares should I buy
            ordershares = int(tradecapital/float(inboundactions[stock].get('price')))

            # reset pricing
            floorprice = 0
            ceilingprice = 0

            if ordershares > 0 and stock not in boughtlist and ordercount < 10:
                # set per-share floor price for bracket order
                floorprice = stockprice - float(traderiskamt/ordershares)

                # set the ceiling price for the bracket order
                family = inboundactions[stock].get('strategyfamily')
                ceilingprice = stockprice * (1 + float(familyreturns[family]))

                # Adjust the floor price if this looks like a bad pick
                if (ceilingprice-stockprice) < float(traderiskamt/ordershares):
                    traderiskamt = (ceilingprice-stockprice) * ordershares
                    floorprice = stockprice - ((ceilingprice-stockprice) * .5) # I never want to break even on risk

                # Add this to the stocks already bought
                boughtlist.append(stock)
                status[stock] = {
                    'datetime': logtime,
                    'ticker': stock,
                    'cash': cash,
                    'TradeRiskAmt': traderiskamt,
                    'TradeCapital': tradecapital,
                    'OrderShares': ordershares,
                    'FloorPrice': floorprice,
                    'CeilingPrice': ceilingprice,
                    'Decision': 'buy'
                }

                # place the order
                try:
                    alpaca.submit_order(
                        side='buy',
                        symbol=stock,
                        type='market',
                        qty=ordershares,
                        time_in_force='gtc', # I think bracket orders need gtc time in force
                        order_class='bracket',
                        take_profit={
                            'limit_price': ceilingprice
                        },
                        stop_loss={
                            'stop_price': floorprice
                        }

                    )
                except Exception as ex:
                    print(stock, ex)
            else:
                if stock not in skiplist:
                    # add this to the skip list -- the timing just wasn't right
                    skiplist.append(stock)
                    status[stock] = {
                        'datetime': logtime,
                        'ticker': stock,
                        'cash': cash,
                        'TradeRiskAmt': traderiskamt,
                        'TradeCapital': tradecapital,
                        'OrderShares': ordershares,
                        'FloorPrice': floorprice,
                        'CeilingPrice': ceilingprice,
                        'Decision': 'skip'
                    }
            ordercount = ol.GetOrderCount()

        #advance the progress bar
        prgbar.next()

    # finish the progress bar
    prgbar.finish()

    # write the bought and skip lists
    with open (buyskippath, 'w', newline='\n', encoding='utf-8') as outfile:
        tmp = {
            'buy': boughtlist,
            'skip': skiplist
        }
        tmp = json.dumps(status, indent=4)
        outfile.write(tmp)

    # update broker status
    with open (statuspath, 'w', newline='\n', encoding='utf-8') as outfile:
        fieldnames = ['datetime', 'ticker', 'cash', 'TradeRiskAmt', 'TradeCapital', 'OrderShares', 'FloorPrice',
                      'CeilingPrice', 'Decision']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        # write the header
        writer.writeheader()
        #writer.writerow(['datetime', 'ticker', 'cash', 'TradeRiskAmt', 'TradeCapital', 'OrderShares', 'FloorPrice', 'CeilingPrice', 'Decision'])
        for x in status:
            writer.writerow(status[x])


    # wait until the next minute before checking again
    ol.WaitForMinute()

# It's the end of the day; cancel orders and quit
alpaca.cancel_all_orders()
alpaca.close_all_positions()





