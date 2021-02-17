import pandas as pd
import json



def options_chain_cleaner(options_chain, only_type=False):
    """
    Takes unformatted option chain csv and returns cleaned up df.
    Specify only_type='Calls' or 'Puts' if only wanting one or other,
    specify False if wanting both and 2 dataframes will be returned,
    calls first and puts second.
    
    i.e. calls, puts = func('file.csv')
    """
    
    options_chain_unfor_df=pd.DataFrame(options_chain.json())
    
    if only_type == 'Calls':
        Calls = options_chain_unfor_df['callExpDateMap']
        call_option_list = []
        for i in Calls:
            for j in i.values():
                for k in j:
                    call_option_list.append(k)
        Calls_df = pd.DataFrame(call_option_list)
        Calls_df.set_index('description', inplace=True)
        return Calls_df
    
    elif only_type == 'Puts':
        Puts = options_chain_unfor_df['putExpDateMap']
        put_option_list = []
        for i in Puts:
            for j in i.values():
                for k in j:
                    put_option_list.append(k)
        Puts_df = pd.DataFrame(put_option_list)
        Puts_df.set_index('description', inplace=True)
        return Puts_df
    
    elif only_type == False:
        Puts=options_chain_unfor_df['putExpDateMap']
        Calls=options_chain_unfor_df['callExpDateMap']
        
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



def to_json(data, write_path):
    """
    Writes data to csv file at specified path.
    """
    with open(write_path, 'a') as file:
        file.write(json.dumps(data, indent=4))


def option_chain_csv_reader(option_chain_csv, write_to_csv=False, write_path=None):
    """
    Takes a cleaned option chain csv file with
    json data. Returns a df sorted by Days 
    to Expiration & Strike Price. Ticker symbol
    in string format is used to 
    """
    with open(option_chain_csv) as read_file:
        data = json.load(read_file)
    chain_df = pd.DataFrame(data)
    chain_df.set_index('DESCRIPTION', inplace=True)
    chain_df = chain_df.sort_values(by=['DAYS_TO_EXPIRATION', 'STRIKE_PRICE'])
    if write_to_csv == True:
        chain_df.to_csv(write_path, mode='a')
    return chain_df
