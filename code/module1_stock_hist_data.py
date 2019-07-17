import pandas as pd
import timeit


# Create Insert Statement Function
'''	Input:  Val w/ individual indexed values that we pass to the function
	Output:  None'''

def sql_insert_function(mydb, val):
        mycursor = mydb.cursor()
        sql_command = '''INSERT INTO SYP_STOCK_DATA (DATE_QUOTE, OPEN, HIGH, CLOSE, LOW, VOLUME, TICKER, PRIMARY_KEY) 
                         VALUES(%s, %s, %s, %s, %s, %s, %s, %s)'''
        mycursor.execute(sql_command, val)
        mydb.commit()
        return None

# GET INDV STOCK DATA
def get_indv_stock_data(ticker, ts):
    stock_query = ts.get_daily(symbol=ticker, outputsize='compact')
    stock_data = stock_query[0]
    print('Ticker => {}'.format(ticker))
    print(stock_data.head(1))
    return None

# GET LIST OF TICKERS IN SYP
def get_tickers(mydb, table): 
    'input:  mydb conn, output:  list stocks'
    sql_query = '''SELECT TICKER
               FROM {};'''.format(table)
    result = pd.read_sql(sql_query, mydb)
    # Return List of Tickers
    return result['TICKER']



# TEST RUN - RUN THROUGH S&P 500
def test_SYP_query(mydb, get_tickers, ts_conn, seconds):
    tickers = get_tickers(mydb)
    for ticker in tickers:
        print('Ticker => {}'.format(ticker))
        stock_query = ts_conn.get_daily(symbol=ticker, outputsize='compact')
        stock_data = stock_query[0] # returns a dataframe
        print(stock_data.head(1), '\n')
        time.sleep(seconds)
    return None 

def get_api_call_delay(start):
    '''Input:   Number of stocks queried = 0-5
                Start time from the loop = 0-60
                Need to reset start time & num stocks each 60 seconds
    '''
    # End time as current time
    end   = timeit.timeit()
    delta = (end - start)
    if delta < 60:
        sleep_time = (60.00 - delta)
        return sleep_time
    else: 
        return 0






