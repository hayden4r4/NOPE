from tda.client import Client
from tda.auth import easy_client
from tda import auth
import TDAapikit as tdkit
import pandas as pd
import numpy as np
import os
from datetime import datetime
from datetime import date
from streamz import Stream
from streamz.dataframe import PeriodicDataFrame as pDataFrame
import hvplot
import asyncio
import atexit
import json
import itertools
from pprint import pprint
import pandas_market_calendars as mcal


# Set Tickers to Pull Data For
equity_tickers = ['SPY', 'PSTH', 'AMD', 'THCB', 'APHA', 'QQQ', 'AAPL', 'PLTR', 'TSM']

# Get Current Date for Both Folder Organization and Getting Market Calendar
today=datetime.now().strftime('%m-%d-%Y')
today_yfirst=datetime.now().strftime('%Y-%m-%d')

# Gets NYSE Market Calendar for Current Day
# market_hours=c.get_hours_for_single_market(Client.Markets.OPTION, date=datetime.strptime(today_yfirst, '%Y-%m-%d').date())
nyse=mcal.get_calendar('NYSE')
market_cal_today=nyse.schedule(start_date=today, end_date=today, tz='America/Chicago')

# Checks if Market is Open Today, if Closed then Script Exits
if market_cal_today.empty==True:
    import sys
    sys.exit('Market is Close Today')

# Directory to Save Data to
directory=fr'A:\Stock Data\{today}'

# Creates Folder in Specified Directory if it Doesn't Already Exist
if not os.path.exists(directory):
    os.makedirs(directory)

# Directories to Save Data to, Organized by Call, Put, or Quote
call_option_chain_directory=fr'{directory}\Call Option Chain'
put_option_chain_directory=fr'{directory}\Put Option Chain'
quote_directory=fr'{directory}\Quotes'

# Creates Folders in Specified Directories if They Don't Already Exist
for ticker in equity_tickers:
    if not os.path.exists(call_option_chain_directory + fr'\{ticker}'):
        os.makedirs(call_option_chain_directory + fr'\{ticker}') 
    if not os.path.exists(put_option_chain_directory + fr'\{ticker}'):
        os.makedirs(put_option_chain_directory + fr'\{ticker}')
    if not os.path.exists(quote_directory + fr'\{ticker}'):
        os.makedirs(quote_directory + fr'\{ticker}')

# Sets Streamz Object
stream=Stream()

# Opens TD Ameritrade Account Info and Sets Webdriver Path for Selenium
account_id=open(r'C:\Python\API Keys\TD\TD_ACCOUNT_ID.txt').read()
consumer_key=open(r'C:\Python\API Keys\TD\TD_CONSUMER_KEY.txt').read()
redirect_uri='http://localhost'
token_path='ameritrade-credentials.json'
geckodriver_path=r'C:\Webdrivers\geckodriver.exe'

# Creates Webdriver for Selenium
def make_webdriver():
    # Import selenium here because it's slow to import
    from selenium import webdriver
    driver = webdriver.Firefox(executable_path=geckodriver_path)
    atexit.register(lambda: driver.quit())
    return driver

# Sets td-api Client Object.
# Will Create Refresh Token with OAUTH and Grab With Selenium
# if it Doesn't Exist in Working Folder.
c = easy_client(consumer_key,
                        redirect_uri,
                        token_path,
                        make_webdriver)




# Sets Dictionaries for Call, Put, and Equity Data with
# format {'Ticker Symbol' : DataFrame of Data}
call_chains = {}
put_chains = {}
equity_quotes = {}

def get_option_chains():
    """
    Gets option chains for specified symbols
    using given ticker symbol list named
    equity_tickers.  Appends to dictionary
    as {'Ticker Symbol' : call chain in DataFrame},
    {'Ticker Symbol' : put chain in DataFrame}
    """
    for ticker in equity_tickers:
        options_chain = c.get_option_chain(symbol=ticker, strike_range=c.Options.StrikeRange.ALL)
        calls_chain, puts_chain = tdkit.options_chain_cleaner(options_chain)
        call_chains[ticker] = pd.DataFrame(calls_chain)
        put_chains[ticker] = pd.DataFrame(puts_chain)


def chain_to_csv():
    """
    Stores option chains to csv using given
    ticker symbol list named equity_tickers
    to name.  Saves with filename:
    {'Ticker Symbol'} Call Option Chain.csv or {'Today's Date'}
    {'Ticker Symbol'} Put Option Chain {'Today's Date'}.csv
    """
    for ticker in equity_tickers:
        call_chains[ticker].to_csv(fr'{call_option_chain_directory}\{ticker}\{ticker} Call Option Chain {today}.csv')
        put_chains[ticker].to_csv(fr'{put_option_chain_directory}\{ticker}\{ticker} Put Option Chain {today}.csv')


def get_quotes():
    """
    Gets quotes for specified equities
    using given ticker symbol list named
    equity_tickers. Appends to dictionary
    as {'Ticker Symbol' : quote in DataFrame}.
    """
    for ticker in equity_tickers:
        quotes = c.get_quotes(symbols=ticker)
        equity_quotes[ticker]=pd.DataFrame(quotes.json())

def quote_to_csv():
    """
    Stores quotes to csv using given
    ticker symbol list named equity_tickers
    to name.  Saves with filename:
    {'Ticker Symbol'} Quote {'Today's Date'}.csv
    """
    for ticker in equity_tickers:
        equity_quotes[ticker].to_csv(fr'{quote_directory}\{ticker}\{ticker} Quote {today}.csv')



def NOPE(call_volumes: float, put_volumes: float, call_deltas: float, put_deltas: float, share_volume: int):
    """
    Calculates NOPE, takes volumes
    and deltas as pandas Series and
    share volume as int.
    """
    result = (np.nansum(((call_volumes*call_deltas).values-(put_volumes*abs(put_deltas)).values)))/share_volume
    return result * 10000



def high_option_checker(option_volume: list or int, share_volume: int):
    """
    Returns a number used to determine
    how "optioned" a ticker is.  A value >
    say 0.4 means NOPE_MAD provides a
    farily good window into predicting
    earnings behavior.
    """
    result = np.nansum(option_volume)*100/share_volume
    return result


# Run Functions to Grab Data and Save to CSV in Organized Folders
get_option_chains()
chain_to_csv()
get_quotes()
quote_to_csv()

# Sets Dictionaries for Call, Put Chains
call_chain_dict={}
put_chain_dict={}
# Import Data From CSV's and Appends to Dictionaries
# with format {'Ticker Symbol' : Dataframe with Data}
for ticker in equity_tickers:    
    call_import_df=pd.read_csv(fr'{call_option_chain_directory}\{ticker}\{ticker} Call Option Chain {today}.csv', index_col='description')
    call_import_df=call_import_df[call_import_df['delta'] != -999.0]
    call_chain_dict[ticker]=call_import_df
    put_import_df=pd.read_csv(fr'{put_option_chain_directory}\{ticker}\{ticker} Put Option Chain {today}.csv', index_col='description')
    put_import_df=put_import_df[put_import_df['delta'] != -999.0]
    put_chain_dict[ticker]=put_import_df

# Set Dictionary for Quote Data
quote_dict={}
# Import Quote Data From CSV's and Appends to Dictionary
# with format {'Ticker Symbol' : Dataframe with Data}
for ticker in equity_tickers:
    quote_import_df=pd.read_csv(fr'{quote_directory}\{ticker}\{ticker} Quote {today}.csv', index_col=0)
    quote_dict[ticker]=quote_import_df.T

# Set Dictionary for Call, Put, Share Volume and Delta Data
call_deltas={}
call_volumes={}
put_deltas={}
put_volumes={}
share_volume={}
# Appends Call, Put, Share Volume and Delta to Dictionaries
# with format {'Ticker Symbol' : option delta or volume as series or share volume as int}
for ticker in equity_tickers:
    call_deltas[ticker]=call_chain_dict[ticker]['delta']
    call_volumes[ticker]=call_chain_dict[ticker]['totalVolume']
    put_deltas[ticker]=put_chain_dict[ticker]['delta']
    put_volumes[ticker]=put_chain_dict[ticker]['totalVolume']
    share_volume[ticker]=int(quote_dict[ticker]['totalVolume'])

# Sets Dictionary for NOPE Values
NOPE_values={}
# Finds NOPE Value Using Function and Appends to Dictionary
# With Format {'Ticker Symbol' : NOPE value}
for ticker in equity_tickers: 
    NOPE_values[ticker]=NOPE(call_volumes=call_volumes[ticker],
                put_volumes=put_volumes[ticker],
                call_deltas=call_deltas[ticker],
                put_deltas=put_deltas[ticker],
                share_volume=share_volume[ticker])

# Sets Dictionaries for Total Sums of Call, Put Delta and Volume
call_delta_sum={}
call_volume_sum={}
put_delta_sum={}
put_volume_sum={}
# Solves for Total Sums of Call, Put Delta and Volume
# and Appends to Dictionaries with format
# {'Ticker Symbol' : option delta or volume}
for ticker in equity_tickers:
    call_delta_sum[ticker]=call_chain_dict[ticker]['delta'].sum()
    call_volume_sum[ticker]=call_chain_dict[ticker]['totalVolume'].sum()
    put_delta_sum[ticker]=put_chain_dict[ticker]['delta'].sum()
    put_volume_sum[ticker]=put_chain_dict[ticker]['totalVolume'].sum()

# Sets Dictionary of Optioned Rate
# Used to Determine How Highly Optioned a Stock is
option_values={}
# Runs Function to Determine the Optioned Rate and append
# to Dictionary with Form {'Ticker Symbol' : optioned rate}
for ticker in equity_tickers:
    option_values[ticker]=high_option_checker(option_volume=[call_volume_sum[ticker], put_volume_sum[ticker]],
                                      share_volume=share_volume[ticker])

# Writes Optioned Rates to File in Specified Directory
with open(fr'{directory}\Option Values.txt', 'w') as file:
    file.write(json.dumps(option_values))

# Writes NOPE Values to File in Specified Directory
with open(fr'{directory}\NOPE Values.txt', 'w') as file:
    file.write(json.dumps(NOPE_values))