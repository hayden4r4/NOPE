from tda.auth import easy_client
from tda import auth
import pandas as pd
import numpy as np
import os
from datetime import datetime
import atexit
import json
import itertools
import threading
import pandas_market_calendars as mcal
from pprint import pprint
from sqlalchemy import create_engine
import psycopg2 as pg


# Set Tickers to Pull Data For
equity_tickers = ['SPY', 'PSTH', 'AMD', 'THCB', 'APHA', 'QQQ', 'AAPL', 'PLTR', 'TSM']

# Get Current Date for Both Folder Organization and Getting Market Calendar
today = datetime.now().strftime('%Y-%m-%d')

# Gets NYSE Market Calendar for Current Day

nyse = mcal.get_calendar('NYSE')
market_cal_today = nyse.schedule(start_date = today, end_date = today, tz = 'America/Chicago')

# Checks if Market is Open Today, if Closed then Script Exits
if market_cal_today.empty == True:
     import sys
     sys.exit('Market is Close Today')

engine = create_engine('postgresql://haydenrose@localhost:5432/stock_data')



# Opens TD Ameritrade Account Info and Sets Webdriver Path for Selenium
account_id=open(r'/Users/haydenrose/Python/API Keys/TD/TD_ACCOUNT_ID.txt').read()
consumer_key=open(r'/Users/haydenrose/Python/API Keys/TD/TD_CONSUMER_KEY.txt').read()
redirect_uri='http://localhost'
token_path=r'/Users/haydenrose/Python/API Keys/TD/ameritrade-credentials.json'
geckodriver_path=r'/Users/haydenrose/Webdrivers/geckodriver'

# Creates Webdriver for Selenium
def make_webdriver():
    # Import selenium here because it's slow to import
    from selenium import webdriver
    driver = webdriver.Firefox(executable_path = geckodriver_path)
    atexit.register(lambda: driver.quit())
    return driver

# Sets td-api Client Object.
# Will Create Refresh Token with OAUTH and Grab With Selenium
# if it Doesn't Exist in Working Folder.
c = easy_client(consumer_key,
                        redirect_uri,
                        token_path,
                        make_webdriver)



def options_chain_cleaner(options_chain, only_type=False):
    """
    Takes unformatted option chain csv and returns cleaned up df.
    Specify only_type='Calls' or 'Puts' if only wanting one or other,
    specify False if wanting both and 2 dataframes will be returned,
    calls first and puts second.

    i.e. calls, puts = func('file.csv')
    """

    if only_type == 'Calls':
        Calls = options_chain['callExpDateMap'].values()
        call_option_list = []
        for i in Calls:
            for j in i.values():
                for k in j:
                    call_option_list.append(k)
        Calls_df = pd.DataFrame(call_option_list)
        Calls_df.set_index('description', inplace=True)
        return Calls_df

    elif only_type == 'Puts':
        Puts = options_chain['putExpDateMap'].values()
        put_option_list = []
        for i in Puts:
            for j in i.values():
                for k in j:
                    put_option_list.append(k)
        Puts_df = pd.DataFrame(put_option_list)
        Puts_df.set_index('description', inplace=True)
        return Puts_df

    elif only_type == False:
        Puts=options_chain['putExpDateMap'].values()
        Calls=options_chain['callExpDateMap'].values()

        call_option_list = []
        for i in Calls:
            for j in i.values():
                for k in j:
                    call_option_list.append(k)
        Calls_df=pd.DataFrame(call_option_list)
        Calls_df.set_index('description', inplace=True)

        put_option_list = []
        for i in Puts:
            for j in i.values():
                for k in j:
                    put_option_list.append(k)
        Puts_df = pd.DataFrame(put_option_list)
        Puts_df.set_index('description', inplace=True)

        return Calls_df, Puts_df

    else:
        raise ValueError('Incorrect only_type value')


# Sets Dictionaries for Call, Put, and Equity Data 
# with format {'Ticker Symbol' : DataFrame of Data}
call_chains = {}
put_chains = {}
equity_quotes = {}

def get_option_chains(ticker):
    """
    Gets option chains for specified symbols
    using given ticker symbol list named
    equity_tickers.  Appends to dictionary
    as {'Ticker Symbol' : call chain in DataFrame},
    {'Ticker Symbol' : put chain in DataFrame}
    """
    
    options_chain = c.get_option_chain(symbol=ticker, strike_range=c.Options.StrikeRange.ALL)
    calls_chain, puts_chain = options_chain_cleaner(options_chain.json())
    calls_chain['date'] = today
    puts_chain['date'] = today
    call_chains[ticker] = calls_chain[calls_chain['delta'] != -999.0]
    put_chains[ticker] = puts_chain[puts_chain['delta'] != -999.0]

    
thread_list=[]
def get_option_chains_threader():
    for ticker in equity_tickers:
        threadProcess = threading.Thread(name='simplethread', target=get_option_chains, args=[ticker])
        thread_list.append(threadProcess)
    for thread in thread_list:
        thread.start()
    for thread in thread_list:
        thread.join()

def get_quotes(ticker):
    """
    Gets quotes for specified equities
    using given ticker symbol list named
    equity_tickers. Appends to dictionary
    as {'Ticker Symbol' : quote in DataFrame}.
    """
    quotes = c.get_quotes(symbols = ticker)
    equity_quotes[ticker] = pd.DataFrame(quotes.json()).T
    equity_quotes[ticker].drop(columns = ['52WkHigh', '52WkLow'], inplace = True)
    equity_quotes[ticker].replace({'': np.nan, ' ': np.nan}, inplace = True)
    
thread_list2=[]
def get_quotes_threader():
    for ticker in equity_tickers:
        threadProcess = threading.Thread(name='simplethread', target=get_quotes, args=[ticker])
        thread_list2.append(threadProcess)
    for thread in thread_list2:
        thread.start()
    for thread in thread_list2:
        thread.join()


def NOPE(call_volumes: float or int, put_volumes: float or int, call_deltas: float, put_deltas: float, share_volume: float or int):
    """
    Calculates NOPE, takes volumes
    and deltas as pandas Series and
    share volume as int.
    """
    result = (sum((((call_volumes*100).mul(call_deltas*100, fill_value=0)).values-((put_volumes*100).mul(abs(put_deltas*100), fill_value=0)).values)))/share_volume
    return result



def high_option_checker(option_volume: list or int or float, share_volume: int):
    """
    Returns a number used to determine
    how "optioned" a ticker is.  A value >
    say 0.4 means NOPE_MAD provides a
    fairly good window into predicting
    earnings behavior.
    """
    result = np.nansum(option_volume)*100/share_volume
    return result

# Set Dictionary for Call, Put, Share Volume and Delta Data
call_deltas={}
put_deltas={}

call_volumes={}
put_volumes={}
share_volume = {}

call_delta_sum = {}
put_delta_sum = {}

call_volume_sum = {}
put_volume_sum = {}

def delta_volumes():
    # Appends Call, Put, Share Volume and Delta to Dictionaries
    # with format {'Ticker Symbol' : option delta or volume as series or share volume as int}
    for ticker in equity_tickers:
        call_deltas[ticker] = call_chains[ticker]['delta'].astype(float)
        put_deltas[ticker] = put_chains[ticker]['delta'].astype(float)

        call_volumes[ticker] = call_chains[ticker]['totalVolume'].astype(float)
        put_volumes[ticker] = put_chains[ticker]['totalVolume'].astype(float)
        share_volume[ticker] = int(equity_quotes[ticker]['totalVolume'])

        call_delta_sum[ticker]=call_chains[ticker]['delta'].astype(float).sum()
        put_delta_sum[ticker]=put_chains[ticker]['delta'].astype(float).sum()

        call_volume_sum[ticker]=call_chains[ticker]['totalVolume'].astype(float).sum()
        put_volume_sum[ticker]=put_chains[ticker]['totalVolume'].astype(float).sum()


def _to_sql():
    """
    Stores option chains to csv using given
    ticker symbol list named equity_tickers
    to name.  Saves with filename:
    {'Ticker Symbol'} Call Option Chain.csv or
    {'Ticker Symbol'} Put Option Chain.csv
    """
    for ticker in equity_tickers:
        call_chain_to_sql = call_chains[ticker].copy()
        call_chain_to_sql.columns = call_chain_to_sql.columns.str.lower()
        call_chain_to_sql.to_sql('option_chains', con = engine, if_exists = 'append')
        
        put_chain_to_sql = put_chains[ticker].copy()
        put_chain_to_sql.columns = put_chain_to_sql.columns.str.lower()
        put_chain_to_sql.to_sql('option_chains', con = engine, if_exists = 'append')
        
        equity_quotes_to_sql = equity_quotes[ticker].copy()
        equity_quotes_to_sql.columns = equity_quotes_to_sql.columns.str.lower()
        equity_quotes_to_sql.to_sql('equity_quotes', con = engine, if_exists = 'append', index = False)
        
        # Finds NOPE Value Using Function and Appends to Dictionary
        # With Format {'Ticker Symbol' : NOPE value} 
        NOPE_value = NOPE(call_deltas = call_deltas[ticker],
                        put_deltas = put_deltas[ticker],
                        call_volumes = call_volumes[ticker],
                        put_volumes = put_volumes[ticker],
                        share_volume = share_volume[ticker])
        
        # Runs Function to Determine the Optioned Rate and append
        # to Dictionary with Form {'Ticker Symbol' : optioned rate}
        optionality = high_option_checker(option_volume = [call_volume_sum[ticker], put_volume_sum[ticker]],
                                                            share_volume = share_volume[ticker])
        #Creates dataframe with NOPE, optionality, and date
        data = {'nopevalue': NOPE_value, 'optionality': optionality, 'symbol': ticker, 'date': today}
        NOPE_valuedf = pd.DataFrame(data, index = [0])
        
        #Saves to SQL table
        NOPE_to_sql = NOPE_valuedf
        NOPE_to_sql.to_sql('nope_values', con = engine, if_exists = 'append', index = False)

get_option_chains_threader()
get_quotes_threader()
delta_volumes()
_to_sql()