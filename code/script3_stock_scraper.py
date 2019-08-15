# PURPOSE:
'''
Scraper the daily open, close and volume for the S&P 500. 

Historical Data Table Key

        Table headers reactionid = 32

        
        date            = 51
        open            = 53
        high            = 55
        low             = 57
        close           = 59
        adj_close       = 61
        volume          = 63

        *Start of next row date is 15 steps.  So the third row is 66 + 15 = 81
        *Stock market opens at 9:30 am and closes at 4pm. 
'''

# IMPORT PYTHON LIBRARIES
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import mysql.connector
from datetime import datetime 



# INSTANTIATE CONNECTION TO MYSQL DB
mydb = mysql.connector.connect(
                host='localhost',
                user='ccirelli2',
                passwd='Work4starr',
                database='SYP_INDEX')


# IMPORT MODULES
import module3_stock_scraper as m3

# URL OBJECTS 
#yahoo_summary_page_url = 'https://finance.yahoo.com/quote/{}?p={}&.tsrc=fin-srch'.format(ticker, ticker)


# MAIN SCRAPER FUNCTION
def main_get_stock_data(ticker, row):
	'''Ticker = ticker for which we want to retreive the data 
           row    = the row in the table of historical data we want to retreive.  1 = today. 
                    365 = this many trading days in the past.'''
	
	# GET HISTORICAL DATA TABLE TO TARGET TICKER
	hist_price_data = m3.get_historical_data_table(mydb, ticker) 
	
	# INDEX
	'''row index starts with 51 or 52.  We use the below to move between dates/rows'''
	row_index = row * 15
	reactid   = row_index + 36
	
	# GET INDIVIDUAL DATA POINTS
	date  = m3.get_stock_data_point(hist_price_data,  row_index, 'date', ticker)
	open_ = m3.get_stock_data_point(hist_price_data,  row_index, 'open', ticker)
	high  = m3.get_stock_data_point(hist_price_data,  row_index, 'high', ticker)
	close = m3.get_stock_data_point(hist_price_data,  row_index, 'close', ticker)
	low   = m3.get_stock_data_point(hist_price_data,  row_index, 'low', ticker)
	volume = m3.get_stock_data_point(hist_price_data, row_index, 'volume', ticker)
		
		
	# DATA TRANSFORMATION
	val = m3.format_val_4_insertion(date, ticker, open_, high, close, low, volume)	
	print(val)	
	# DATA INSERTION INTO DB 
	#m3.sql_insert_stock_data(mydb, val, ticker)
	
	


# RUN LOOP OVER SYP 500 TICKER LIST

# ***The precense of the dividend is changing the count. 
# ***We need to figure out a way to identify the dividend row
# *** and accomodate this with a change to the count without breaking the loop


syp_tickers = m3.get_tickers(mydb, 'SYP_TICKERS')
m3.clear_table(mydb, 'SYP_STOCK_DATA')
m3.clear_table(mydb, 'SYP_STOCK_DATA_LOG')
# Loop Over Tickers
for ticker in syp_tickers:
	print('Scraping started for ticker => {}'.format(ticker))
	for day in range(1,365):
		main_get_stock_data(ticker, day)
		if day == 365:
			print('Scraping finished for ticker => {}\n'.format(ticker))







