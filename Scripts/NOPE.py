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


# Set Tickers to Pull Data For
equity_tickers = ['SPY', 'PSTH', 'AMD', 'THCB', 'APHA', 'QQQ', 'AAPL', 'PLTR', 'TSM']

# Get Current Date for Both Folder Organization and Getting Market Calendar
today = datetime.now().strftime('%m-%d-%Y')
today_yfirst = datetime.now().strftime('%Y-%m-%d')

# Gets NYSE Market Calendar for Current Day
# 
nyse = mcal.get_calendar('NYSE')
market_cal_today = nyse.schedule(start_date = today, end_date = today, tz = 'America/Chicago')

# Checks if Market is Open Today, if Closed then Script Exits
# if market_cal_today.empty == True:
#     import sys
#     sys.exit('Market is Close Today')

# Directory to Save Data to
directory=r'A:\Stock Data'

# Creates Folder in Specified Directory if it Doesn't Already Exist
if not os.path.exists(directory):
    os.makedirs(directory)

# Directories to Save Data to, Organized by Call, Put, or Quote
call_option_chains_directory = fr'{directory}\Call Option Chains'
put_option_chains_directory = fr'{directory}\Put Option Chains'
quotes_directory = fr'{directory}\Quotes'
optioned_directory = fr'{directory}\Optioned Values'
NOPE_directory = fr'{directory}\NOPE Values'

# Creates Folders in Specified Directories if They Don't Already Exist
for ticker in equity_tickers:
    if not os.path.exists(call_option_chains_directory):
        os.makedirs(call_option_chains_directory) 
    if not os.path.exists(put_option_chains_directory):
        os.makedirs(put_option_chains_directory)
    if not os.path.exists(quotes_directory):
        os.makedirs(quotes_directory)
    if not os.path.exists(optioned_directory):
        os.makedirs(optioned_directory)
    if not os.path.exists(NOPE_directory):
        os.makedirs(NOPE_directory)


# Opens TD Ameritrade Account Info and Sets Webdriver Path for Selenium
account_id=open(r'C:\Python\API Keys\TD\TD_ACCOUNT_ID.txt').read()
consumer_key=open(r'C:\Python\API Keys\TD\TD_CONSUMER_KEY.txt').read()
redirect_uri='http://localhost'
token_path=r'C:\Python\API Keys\TD\ameritrade-credentials.json'
geckodriver_path=r'C:\Webdrivers\geckodriver.exe'

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
    calls_chain['Date'] = today
    puts_chain['Date'] = today
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


def chain_to_csv():
    """
    Stores option chains to csv using given
    ticker symbol list named equity_tickers
    to name.  Saves with filename:
    {'Ticker Symbol'} Call Option Chain.csv or
    {'Ticker Symbol'} Put Option Chain.csv
    """
    for ticker in equity_tickers:
        call_chains[ticker].to_csv(fr'{call_option_chains_directory}\{ticker} Call Option Chains.csv', mode = 'a', header = not os.path.exists(fr'{call_option_chains_directory}\{ticker} Call Option Chains.csv'))
        put_chains[ticker].to_csv(fr'{put_option_chains_directory}\{ticker} Put Option Chains.csv', mode = 'a', header = not os.path.exists(fr'{put_option_chains_directory}\{ticker} Put Option Chains.csv'))


def get_quotes(ticker):
    """
    Gets quotes for specified equities
    using given ticker symbol list named
    equity_tickers. Appends to dictionary
    as {'Ticker Symbol' : quote in DataFrame}.
    """
    quotes = c.get_quotes(symbols=ticker)
    equity_quotes[ticker]=pd.DataFrame(quotes.json()).T
thread_list2=[]
def get_quotes_threader():
    for ticker in equity_tickers:
        threadProcess = threading.Thread(name='simplethread', target=get_quotes, args=[ticker])
        thread_list2.append(threadProcess)
    for thread in thread_list2:
        thread.start()
    for thread in thread_list2:
        thread.join()

def quote_to_csv():
    """
    Stores quotes to csv using given
    ticker symbol list named equity_tickers
    to name.  Saves with filename:
    {'Ticker Symbol'} Quote.csv
    """
    for ticker in equity_tickers:
        equity_quotes[ticker].to_csv(fr'{quotes_directory}\{ticker} Quotes.csv', mode = 'a', header = not os.path.exists(fr'{quotes_directory}\{ticker} Quotes.csv'))



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


# Run Functions to Grab Data and Save to CSV in Organized Folders
get_option_chains_threader()
chain_to_csv()
get_quotes_threader()
quote_to_csv()


# Set Dictionary for Call, Put, Share Volume and Delta Data
call_deltas={}
put_deltas={}
call_volumes={}
put_volumes={}
share_volume = {}
# Appends Call, Put, Share Volume and Delta to Dictionaries
# with format {'Ticker Symbol' : option delta or volume as series or share volume as int}
for ticker in equity_tickers:
    call_deltas[ticker] = call_chains[ticker]['delta'].astype(float)
    put_deltas[ticker] = put_chains[ticker]['delta'].astype(float)
    call_volumes[ticker] = call_chains[ticker]['totalVolume'].astype(float)
    put_volumes[ticker] = put_chains[ticker]['totalVolume'].astype(float)
    share_volume[ticker] = int(equity_quotes[ticker]['totalVolume'])


# Finds NOPE Value Using Function and Appends to Dictionary
# With Format {'Ticker Symbol' : NOPE value} 
for ticker in equity_tickers: 
    NOPE_value = NOPE(call_deltas = call_deltas[ticker],
                    put_deltas = put_deltas[ticker],
                    call_volumes = call_volumes[ticker],
                    put_volumes = put_volumes[ticker],
                    share_volume = share_volume[ticker])
    NOPE_valuedf = pd.DataFrame(NOPE_value, index=[today], columns=['NOPE Value'])
    NOPE_valuedf['Symbol'] = ticker
    NOPE_valuedf.to_csv(fr'{NOPE_directory}\{ticker} NOPE Values.csv', mode = 'a', header = not os.path.exists(fr'{NOPE_directory}\{ticker} NOPE Values.csv'))

# Sets Dictionaries for Total Sums of Call, Put Delta and Volume
call_delta_sum = {}
call_volume_sum = {}
put_delta_sum = {}
put_volume_sum = {}
# Solves for Total Sums of Call, Put Delta and Volume
# and Appends to Dictionaries with format
# {'Ticker Symbol' : option delta or volume}
for ticker in equity_tickers:
    call_delta_sum[ticker]=call_chains[ticker]['delta'].astype(float).sum()
    call_volume_sum[ticker]=call_chains[ticker]['totalVolume'].astype(float).sum()
    put_delta_sum[ticker]=put_chains[ticker]['delta'].astype(float).sum()
    put_volume_sum[ticker]=put_chains[ticker]['totalVolume'].astype(float).sum()


# Runs Function to Determine the Optioned Rate and append
# to Dictionary with Form {'Ticker Symbol' : optioned rate}
for ticker in equity_tickers:
    optioned_valuesdf = pd.DataFrame(high_option_checker(option_volume = [call_volume_sum[ticker], put_volume_sum[ticker]],
                                                        share_volume = share_volume[ticker]),
                                        index = [today],
                                        columns = ['Optioned Value'])
    optioned_valuesdf['Date'] = today
    optioned_valuesdf['Symbol'] = ticker
    optioned_valuesdf.to_csv(fr'{optioned_directory}\{ticker} Optioned Values.csv', mode = 'a', header = not os.path.exists(fr'{optioned_directory}\{ticker} Optioned Values.csv'))