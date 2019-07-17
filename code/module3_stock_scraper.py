# IMPORT PYTHON LIBRARIES
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import mysql.connector
from datetime import datetime
import time


# INSTANTIATE CONNECTION TO MYSQL DB
mydb = mysql.connector.connect(
                host='localhost',
                user='ccirelli2',
                passwd='Work4starr',
                database='SYP_INDEX')


def clear_table(mydb, table):
	mycursor = mydb.cursor()
	sql_command = 'DELETE FROM {};'.format(table)
	mycursor.execute(sql_command)
	mydb.commit()
	print('Table => {} cleared successfully'.format(table))


def insert_log_error_data(mydb, err, fname, ticker):
	# Log data	
	time_of_error = str(datetime.today()).replace(' ', '_').split('_')[1][:8]
	date_of_error = str(datetime.today().date())
	pkey = ticker + '_' + time_of_error
	function = fname
	err_str = str(err)	
	val = (time_of_error, date_of_error, ticker, err_str, pkey, function)
	
	# Insertion 
	mycursor = mydb.cursor()
	sql_command = '''INSERT INTO SYP_STOCK_DATA_LOG
                        (TIME_, DATE_, TICKER, ERROR_MESSAGE, PKEY, FUNCTION)
	                        VALUES(%s, %s, %s, %s, %s, %s)'''
	# Execute
	try:
		mycursor.execute(sql_command, val)
		mydb.commit()
	# Except Mysql Error
	except mysql.connector.Error as err:
		print('Error in log insert function.  Error message => {}'.format(err))
	# Add One Second Delay
	time.sleep(1)
	return None



def get_historical_data_table(mydb, ticker):
	'''Obtains historical stock data table for the ticker in question'''
	# Historical Data URL
	Url = 'https://finance.yahoo.com/quote/{}/history?p={}'.format(ticker, ticker)
	# Open Yahoo Historical Data Page
	html = urlopen(Url)
	# Create Beatutiful Soup Object
	bsObj = BeautifulSoup(html.read(), 'lxml')
	# Get Entire Table of Historical Prices
	tags = bsObj.find('table', {'data-test':'historical-prices'})
	try:
		# Get Body of Values
		hist_price_data = tags.find('tbody')
		# return 
		return hist_price_data
	except AttributeError as err:
		print('Get_historical_data_table function generated an error => {}'.format(err))
		insert_log_error_data(mydb, err, 'get_historical_data_table', ticker)	
		print('Error successfully logged')



def get_stock_data_point(hist_price_data, row_index, data_point, ticker):
	try:
		if data_point == 'date':
			return hist_price_data.find('span', {'data-reactid':'{}'\
						      	.format((51-15)+row_index)}).get_text()
		elif data_point == 'open':
			return hist_price_data.find('span', {'data-reactid':'{}'\
								.format((53-15)+row_index)}).get_text().replace(',','')
		elif data_point == 'high':
			return hist_price_data.find('span', {'data-reactid':'{}'\
								.format((55-15)+row_index)}).get_text().replace(',','')
		elif data_point == 'low':
			return hist_price_data.find('span', {'data-reactid':'{}'\
								.format((57-15)+row_index)}).get_text().replace(',','')
		elif data_point == 'close':
			return hist_price_data.find('span', {'data-reactid':'{}'\
								.format((59-15)+row_index)}).get_text().replace(',','')
		elif data_point == 'adj_close':
			return hist_price_data.find('span', {'data-reactid':'{}'\
								.format((61-15)+row_index)}).get_text().replace(',','')
		elif data_point == 'volume':
			return hist_price_data.find('span', {'data-reactid':'{}'\
								.format((63-15)+row_index)}).get_text().replace(',','')
		else:
			print('Warning:  Incorrect input value for data_point')
	
	except AttributeError as err:
		insert_log_error_data(mydb, err, 'get_stock_data_point', ticker)	

	return None



def get_tickers(mydb, table):
    'input:  mydb conn, output:  list stocks'
    sql_query = '''SELECT TICKER
               FROM {};'''.format(table)
    result = pd.read_sql(sql_query, mydb)
    # Return List of Tickers
    return result['TICKER']



def sql_insert_stock_data(mydb, val, ticker):
	mycursor = mydb.cursor()
	sql_command = '''INSERT INTO SYP_STOCK_DATA 
			(DATE_QUOTE, OPEN, HIGH, CLOSE, LOW, VOLUME, TICKER, PRIMARY_KEY) 
                         VALUES(%s, %s, %s, %s, %s, %s, %s, %s)'''
	try:
		mycursor.execute(sql_command, val)
		mydb.commit()
	except mysql.connector.Error as err:
		function = 'sql_insert_stock_data'
		insert_log_error_data(mydb, err, function, ticker)
	return None

def get_current_time():
	hour = datetime.now().hour
	minute = datetime.now().minute
	second = datetime.now().second
	return '{}:{}:{}'.format(hour, minute, second)




def format_val_4_insertion(date, ticker, open_, high, close, low, volume):
	try:
		format_date = datetime.strptime(date, '%b %d, %Y')
		pkey = ticker + '_' + str(format_date.date())
		val = (format_date, open_, high, close, low, volume, ticker, pkey) 
		return val

	except ValueError as err:
		print('Warning:  Error => {}'.format(err))
		insert_log_error_data(mydb, err, 'get_stock_data_point', ticker)
		print('Error successfully logged')
	
	except TypeError as err:
		print('Warning:  Error => {}'.format(err))
		insert_log_error_data(mydb, err, 'get_stock_data_point', ticker)
		print('Error successfully logged')









	
