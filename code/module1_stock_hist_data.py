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


