# The purpose of this script is to test some of the more basic functions of the yahoo fin api

'''	API DOCS:		https://www.alphavantage.co/
  	API_KEY:		0NVCHF5MD13R11PG
	GitHub Repo:  	https://github.com/RomelTorres/alpha_vantage
	API DOCS:		outputsize:	can either be 'full' or 'compact'.  Initial commit we use full
	API Output:		Output is a tuple object where the data is located in pos index[0]
					date, 1. open, 2. high, 4. close, 3. low, 5. volume
	SYP_STOCK_DATA:	DATE_QUOTE, OPEN, HIGH, CLOSE, LOW, VOLUME, TICKER, PRIMARY_KEY	
'''

# IMPORT ALPHA VANTAGE API LIBRARY
from alpha_vantage.timeseries import TimeSeries 
import mysql.connector


# INSTANTIATE CONNECTION TO MYSQL DATABASE
mydb = mysql.connector.connect(
                host='localhost',
                user='ccirelli2',
                passwd='Work4starr',
                database='SYP_INDEX')

# IMPORT PYTHON LIBRARIES
import pandas as pd
import os
import module1_stock_hist_data as m1

# INSTANTIATE CONNECTION TO ALPHA VANTAGE API
ts = TimeSeries(key='0NVCHF5MD13R11PG', output_format='pandas', indexing_type='date')

# GET INDV STOCK DATA
def get_indv_stock_data(ticker):
    stock_query = ts.get_daily(symbol=ticker, outputsize='compact')
    stock_data = stock_query[0]
    for row in stock_data.itertuples():
        print(row)


# GET LIST OF TICKERS IN SYP
def get_tickers(mydb): 
	sql_query = '''SELECT TICKER
		       FROM SYP_TICKERS;'''
	result = pd.read_sql(sql_query, mydb)
	# Return List of Tickers
	return result['TICKER']
	
# FUNCTION QUERY HISTORICAL STOCK DATA (INITIAL COMMIT)
def main_hist_stock_data_commit(mydb):
	'Purpose:  Adds historical data to table'
	# Generate list of tickers
	list_tickers = get_tickers(mydb)	
	# Count Object
	Count = 1
	# Iterate List 
	for ticker in list_tickers:
		# User Feedback
		print('Starting stock data insertion for {}, which is {} of {}'.format(ticker, Count, len(list_tickers)))
		# Get Historical Stock Data
		'''Note:  ts is the instantiation of TimeSeries using key and output format'''
		stock_query = ts.get_daily(symbol=ticker ,outputsize='full')
		stock_data = stock_query[0]
		# Iterate the Individual Stock Data Table & Generate A Tuple
		for row in stock_data.itertuples():
			# Insert Data Into MySQL Table
			pkey = str(row[0]) + '_' + ticker
			'''date, volume, open, high, close, low'''
			val = (row[0],row[2], row[3], row[4], row[5], row[1], ticker, pkey) 
			try:
				m1.sql_insert_function(mydb, val)

			except mysql.connector.errors.IntegrityError as err:
				print('Error generated => {}'.format(err))

		# Completion of Insert
		print('Ticker => {} commited'.format(ticker))
		# Increment Count
		Count +=1


main_hist_stock_data_commit(mydb)
