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

# INSTANTIATE CONNECTION TO MYSQL DATABASE
import mysql.connector
mydb = mysql.connector.connect(
                host='localhost',
                user='ccirelli2',
                passwd='Work4starr',
                database='SYP_INDEX')

# IMPORT PYTHON LIBRARIES
import pandas as pd
import os
import module1_stock_hist_data as m1
import time
import timeit 
import logging 

# INSTANTIATE CONNECTION TO ALPHA VANTAGE API______________________________________
ts = TimeSeries(key='0NVCHF5MD13R11PG', output_format='pandas', indexing_type='date')

	
# FUNCTION QUERY HISTORICAL STOCK DATA (INITIAL COMMIT)____________________________

def main_hist_stock_data_commit(mydb):
	'''Purpose:  Adds historical data to table
	   07.06.19:	Adding a timer to not exceed 5 API calls per minute (free tier)'''

	# Generate list of tickers-----------------------------------------------------
	'''Get list of all SYP tickers, limit api calls to only those not in table'''
	SYP_INDEX_tickers = m1.get_tickers(mydb, 'SYP_TICKERS')
	SYP_STOCK_DATA_tickers = [x for x in m1.get_tickers(mydb, 'SYP_STOCK_DATA')]	
	remaining_list_tickers = [x for x in SYP_INDEX_tickers if x not in SYP_STOCK_DATA_tickers]	
	# Timer & Count Object---------------------------------------------------------
	Count = 1
	start = timeit.timeit()

	# Iterate List-----------------------------------------------------------------
	for ticker in remaining_list_tickers:
		
		# Get Historical Stock Data------------------------------------------------
		
		# Check API Calls Per Minute (Max 5)
		if Count == 5:
			# Get Sleep Delay (if runtime < 60 seconds)
			sleep_delay = m1.get_api_call_delay(start)
			if sleep_delay != 0:
				print('Sleeping for {} seconds'.format(sleep_delay))
				time.sleep(sleep_delay)
		 	# Reset Count & Start time 
			Count = 1
			start = timeit.timeit()

		# Call API For Given Ticker------------------------------------------------
		try:	
			print('Calling API for ticker {}'.format(ticker))
			stock_query = ts.get_daily(symbol=ticker ,outputsize='full')
			stock_data = stock_query[0]
			
			# Iterate the Individual Stock Data Table & Generate A Tuple
			print('Starting insertion for ticker {}'.format(ticker))
			for row in stock_data.itertuples():

				# Insert Data Into MySQL Table
				pkey = str(row[0]) + '_' + ticker
				val = (row[0],row[2], row[3], row[4], row[5], row[1], ticker, pkey)
				# Try Insertion 
				try:
					m1.sql_insert_function(mydb, val)
				# Except MySQL Error
				except mysql.connector.errors.IntegrityError as err:
					print('Error generated => {}'.format(err))
		
			# User Feedback
			print('Ticker => {} committed'.format(ticker))
			Count +=1

		# Except API Errors-------------------------------------------------------
		except ValueError as err:
			print('Error generated => {}'.format(err))
			Count +=1

		except KeyError as err:
			print('Error generated => {} for stock {}'.format(err, ticker))
			Count +=1
		
	# End of Function----------------------------------------------------------------	
	return None



# RUN MAIN FUNCTION______________________________________________________________________
stock_api_function = main_hist_stock_data_commit(mydb)


# Generate Log File----------------------------------------------------------------------
print('\n Generating loggin file')
logging_output_dir = '/home/ccirelli2/Desktop/repositories/stock_api/logging'
logging.basicConfig(
        filename = logging_output_dir + 'log.txt',
        level=logging.DEBUG,
        format='%(asctime)s:%(levelname)s:%(message)s')
print('Log files saved to {}'.format(logging_output_dir))
logging.debug(stock_api_function)













