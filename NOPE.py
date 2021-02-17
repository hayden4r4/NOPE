from tda.client import Client
from tda.auth import easy_client
from tda import auth
import TDAapikit as tdkit
import pandas as pd
import numpy as np
import os
from datetime import datetime
import json
import itertools
from pprint import pprint



equity_tickers = ['PSTH', 'SPY']


stream=Stream()

today=datetime.now().strftime('%m-%d-%Y')
today_yfirst=datetime.now().strftime('%Y-%m-%d')

directory=fr'A:\Stock Data\{today}'

if not os.path.exists(directory):
    os.makedirs(directory)


call_option_chain_directory=fr'{directory}\Call Option Chain'
put_option_chain_directory=fr'{directory}\Put Option Chain'
quote_directory=fr'{directory}\Quotes'

for ticker in equity_tickers:
    if not os.path.exists(call_option_chain_directory + fr'\{ticker}'):
        os.makedirs(call_option_chain_directory + fr'\{ticker}') 
    if not os.path.exists(put_option_chain_directory + fr'\{ticker}'):
        os.makedirs(put_option_chain_directory + fr'\{ticker}')
    if not os.path.exists(quote_directory + fr'\{ticker}'):
        os.makedirs(quote_directory + fr'\{ticker}')



account_id=open(r'C:\Python\API Keys\TD\TD_ACCOUNT_ID.txt').read()
consumer_key=open(r'C:\Python\API Keys\TD\TD_CONSUMER_KEY.txt').read()
redirect_uri='http://localhost'
token_path='ameritrade-credentials.json'
geckodriver_path=r'C:\Webdrivers\geckodriver.exe'


def make_webdriver():
    # Import selenium here because it's slow to import
    from selenium import webdriver

    driver = webdriver.Firefox(executable_path=geckodriver_path)
    atexit.register(lambda: driver.quit())
    return driver


c = easy_client(consumer_key,
                        redirect_uri,
                        token_path,
                        make_webdriver)


market_hours=c.get_hours_for_single_market(Client.Markets.OPTION, date=datetime.strptime(today_yfirst, '%Y-%m-%d').date())


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
    return result



def high_option_checker(option_volume: list or int, share_volume: int):
    """
    Returns a number used to determine
    how "optioned" a ticker is.  A value >
    say 0.4 is means NOPE_MAD provides a
    farily good window into predicting
    earnings behavior.
    """
    result = np.nansum(option_volume)*100/share_volume
    return result



get_option_chains()
chain_to_csv()
get_quotes()
quote_to_csv()


call_chain_dict={}
put_chain_dict={}
for ticker in equity_tickers:    
    call_import_df=pd.read_csv(fr'{call_option_chain_directory}\{ticker}\{ticker} Call Option Chain {today}.csv', index_col='description')
    call_import_df=call_import_df[call_import_df['delta'] != -999.0]
    call_chain_dict[ticker]=call_import_df
    put_import_df=pd.read_csv(fr'{put_option_chain_directory}\{ticker}\{ticker} Put Option Chain {today}.csv', index_col='description')
    put_import_df=put_import_df[put_import_df['delta'] != -999.0]
    put_chain_dict[ticker]=put_import_df

quote_dict={}

for ticker in equity_tickers:
    quote_import_df=pd.read_csv(fr'{quote_directory}\{ticker}\{ticker} Quote {today}.csv', index_col=0)
    quote_dict[ticker]=quote_import_df.T


call_deltas={}
call_volumes={}
put_deltas={}
put_volumes={}
share_volume={}

for ticker in equity_tickers:
    call_deltas[ticker]=call_chain_dict[ticker]['delta']
    call_volumes[ticker]=call_chain_dict[ticker]['totalVolume']
    put_deltas[ticker]=put_chain_dict[ticker]['delta']
    put_volumes[ticker]=put_chain_dict[ticker]['totalVolume']
    share_volume[ticker]=int(quote_dict[ticker]['totalVolume'])


NOPE_values={}

for ticker in equity_tickers: 
    NOPE_values[ticker]=NOPE(call_volumes=call_volumes[ticker],
                put_volumes=put_volumes[ticker],
                call_deltas=call_deltas[ticker],
                put_deltas=put_deltas[ticker],
                share_volume=share_volume[ticker])


call_delta_sum={}
call_volume_sum={}
put_delta_sum={}
put_volume_sum={}

for ticker in equity_tickers:
    call_delta_sum[ticker]=call_chain_dict[ticker]['delta'].sum()
    call_volume_sum[ticker]=call_chain_dict[ticker]['totalVolume'].sum()
    put_delta_sum[ticker]=put_chain_dict[ticker]['delta'].sum()
    put_volume_sum[ticker]=put_chain_dict[ticker]['totalVolume'].sum()

option_values={}

for ticker in equity_tickers:
    option_values[ticker]=high_option_checker(option_volume=[call_volume_sum[ticker], put_volume_sum[ticker]],
                                      share_volume=share_volume[ticker])


with open(fr'{directory}\Option Values.txt', 'w') as file:
    file.write(json.dumps(option_values))


with open(fr'{directory}\NOPE Values.txt', 'w') as file:
    file.write(json.dumps(NOPE_values))
