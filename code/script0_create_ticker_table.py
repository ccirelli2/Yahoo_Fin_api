# PURPOSE
'''Purpose is to insert the S&P Ticker Data into our SYP_TICKERS table'''


# IMPORT MODULES
import pandas as pd
import mysql.connector

# INSTANTIATE MYSQL CONNECTION
mydb = mysql.connector.connect(
		host='localhost', 
		user='ccirelli2', 
		passwd='Work4starr', 
		database='SYP_INDEX')

# Create DataFrame of Excel File
df = pd.read_excel('/home/ccirelli2/Desktop/repositories/stock_api/data/SYP_Tickers.xls')

# Create Insert Statement Function
def sql_insert_function(mydb, val):
	mycursor = mydb.cursor()
	sql_command = '''INSERT INTO SYP_TICKERS (company_name, ticker, weight, sector) 
			 VALUES(%s,%s, %s, %s)'''
	mycursor.execute(sql_command, val)
	mydb.commit()
	return None

def sql_whipe_table(mydb, table):
	mycursor = mydb.cursor()
	sql_command = '''DELETE FROM SYP_TICKERS WHERE weight < 100'''
	mycursor.execute(sql_command)
	mydb.commit()
	print('Whipe of table {} complete'.format(table))
	return None

# Iterate DataFrame by row
def main_insert_function(dataFrame):
	'''     
	row[1] = company_name
        row[2] = ticker
        row[3] = weight
        row[4] = sector'''

	Count  = 0
	for row in dataFrame.itertuples():
		val = (row[1], row[2], row[3], row[4])
		sql_insert_function(mydb, val)
		Count +=1
		print('Count insertion = {}'.format(Count))

	print('Insertion complete')

#sql_whipe_table(mydb, 'SYP_TICKERS')
#main_insert_function(df)









