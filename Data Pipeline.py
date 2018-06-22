# -*- coding: utf-8 -*-
"""
Created on Tue Jun 19 17:16:59 2018

@author: Xun Cao
"""

import json
import requests
import pandas as pd
import mysql.connector
import time
from sqlalchemy import create_engine


# Part 1
#step 1
#task 1: fetch the data from the API and save the result(in Json format) into a variable
def apiResult(url):
    response = requests.get(url)
    #Deserialize the response to a python object
    json_data = json.loads(response.text)
    return json_data
url1 = "https://www.septastats.com/api/current/lines"
data1=apiResult(url1)
print(data1)


#task 2: transform the raw data
# truncate the first few rows to exlcude the the metadata and data headers
# add back the meaning full headers and improve the data readbility

# converting json dataset from dictionary to dataframe
# The "data" for for step 1 contains a dict as its value while the "data" in step 2 contains a list of dict, we should handle them  differently

def getdf( js, n, headers ):
	# initialize column names
	col_names = range(2)
	my_df = pd.DataFrame( columns=col_names )
    #iterate through the json file and take out every key-value pairs
    #two keys: metadata and data for js=data1
	for k, v in js.items():
		my_df.loc[len(my_df), 0] = k
		# for step 1
		if isinstance( v, dict ):
			for k1, v1 in v.items():
				my_df.loc[len(my_df)] = [ k1, v1 ]
		# for step 2
		elif isinstance(v, list):
         #turn the list to the data frame first
			sub_df = pd.DataFrame( v ).transpose()
			sub_df.reset_index(inplace=True)
			sub_df.columns = range( len( sub_df.columns ) )
			my_df = pd.concat( [ my_df, sub_df ], ignore_index=True )
   #take out the first n rows of the data         
	my_df = my_df.loc[n:]
	# delete empty columns
	my_df.dropna( axis=1, how='all', inplace=True )
	my_df = my_df.fillna('')	# replace NA with empty string ''
	# first check if headers is passed to this function
	if len( headers ) == 0:
		return my_df
    # check if number os headers matches the number of columns
   # if the numbers are not matched - report error and do nothing
	elif len( headers ) != len( my_df.columns ):
		print( "Incorrect number of headers!" )
		return None
	else:
      #assign the column name as given
		my_df.columns = headers
		return my_df
 
#print out the result for task2
df1 = getdf(data1, 3, [ "line_name", "description" ])
print(df1)

# task 3
try:
    import pymysql
    pymysql.install_as_MySQLdb()
except ImportError:
    pass


engine = create_engine("mysql://root:19921013@kara@localhost:3306/SEPTA_test?charset=utf8",encoding="utf-8", echo=True)
cnx_mine = mysql.connector.connect(host='localhost',port=3306,user='root',
                            password='19921013@kara',charset='utf8')
def constructDB (cnx,tableName, dataSource):
    cnx = cnx
    cursor = cnx.cursor()
    # for step 1: when there are only 2 columns in the data frame
    if len(dataSource.columns) == 2:
     #create a new database: SEPTA_test
        cursor.execute("CREATE DATABASE SEPTA_test")
     #create and initialize a new table, tablename as given   
        sql_createTable = "CREATE TABLE IF NOT EXISTS "+tableName+" (line_name TEXT NOT NULL, description TEXT)"
        try:
            #specify the cursor to use the new database
            cursor.execute("USE SEPTA_test")
            cursor.execute(sql_createTable) 
        #error tracking
        except mysql.connector.Error as e:
            print('create table orange fails!{}'.format(e)) 
            dataSource.to_sql(tableName,con=engine,if_exists='replace')
    #for step 2        
    else:
        #id as the primary key
        sql_createTable2 = "CREATE TABLE IF NOT EXISTS "+tableName+" (id TINYTEXT NOT NULL, time DATETIME NOT NULL, late TINYINT NOTNULL, lat  DECIMAL(10,7) NOT NULL,  lon  DECIMAL(15,12) NOT NULL, nextstop VARCHAR(30) NOT NULL, source VARCHAR(30) NOT NULL, dest VARCHAR(30) NOT NULL, PRIMARY KEY (id))"
        try:
            cursor.execute("USE SEPTA_test")
            cursor.execute(sql_createTable2) 
        #error tracking
        except mysql.connector.Error as e:
            print('create table orange fails!{}'.format(e)) 
            dataSource.to_sql(tableName,con=engine,if_exists='replace')
    return dataSource

# step 2: fetch the data from the API to get the latest reported information for each line in each direction
# task 1:run the call
# task 1
url2 = "https://www.septastats.com/api/current/line/airport/inbound/latest"
data2 = apiResult(url2)
print(data2)

# task 2
df2 = getdf(data2, 6, [])
print(df2)

# task 3: store the data into the database as a new table
# transform the result from getdf before storing it to the database to keep the consistency of the tabular form


sort_key_list = ['time','id', 'source', 'dest', 'nextstop', 'late', 'lat', 'lon']
def preprocessdf( df ):
	# set first column as column names and transpose
	df = df.set_index( [0] )
	df = df.transpose()
	# convert 'time' column to datetime format
	df['time'] = pd.to_datetime( df['time'] )
	# convert lat and lon to float/ decimal format
	df['lat'] = pd.to_numeric( df['lat'] )
	df['lon'] = pd.to_numeric( df['lon'] )
	df.sort_values( sort_key_list, ascending=True )		# sort the data by time
	return df



# Part 2

import time
url_lines = "https://www.septastats.com/api/current/lines"

# get a certain column from a table, return a list
def getcol(table_name, colname):
	pass

# add data to a certain table, data is a dictionary represents a data point
def addrows( table_name, data ):
	pass

# get last row from the table, return a dictionary
def fetch_last_row(table_name):
	pass

# get the row index of lastrow in DataFrame df, if lastrow is not in df, return 0, assume df is already sorted by sort_key_list
def get_row_id(df, lastrow):
	sets = [ set( [ i for i, e in enumerate( list( df[k] ) ) if e == lastrow[k] ] ) for k in sort_key_list ]
	u = set.intersection( *sets )
	if len( u ) == 0:
		return 0
	else:
		return max(u) + 1


def update_table( freq ):
	while True:
		# update the tables every freq mins
		time.sleep( 60 * freq )
		# update line_details table
		lines_info = apiResult( url_lines )['data']
		# current lines fetched from the internet
		line_ext = set( lines_info.keys() )
		# lines stored in the database
		line_int = set( getcol( "line_details", "line_name" ) )
		new_line = line_ext - line_int
		# add new lines(if any) to the line_details table
		for x in new_line:
			addrows(  "line_details", { "line_name":x, "description":lines_info[x] } )
			# create new tables for new lines in the database
			for direction in ["inbound", "outbound"]:
				url = "https://www.septastats.com/api/current/line/{}/{}/latest".format( x, direction )
				table_name = "{}_{}".format( x, direction )
				data = apiResult( url )
				df = getdf( data, 6, [] )
				df = preprocessdf( df )
				# upload table to the database
				pass
		# update train information tables, we don't need to update newly created ones
		all_lines = line_int
		for line in all_lines:
			for direction in ["inbound", "outbound"]:
				# create dataframe for newly fetched data and sort them by colnames in a given order
				url = "https://www.septastats.com/api/current/line/{}/{}/latest".format( x, direction )
				data = apiResult( url )
				df = getdf( data, 6, [] )
				df = preprocessdf( df )
				# get the last row from the information table in the database
				table_name = "{}_{}".format( row[0], direction )
				lastrow = fetch_last_row(table_name)
				# compare lastrow with the dataframe df
				# check if lastrow is in df, if True, return row number, else return 0
				n = get_row_id(df, lastrow)
				# add new rows to the table
				if n < len(df):
					df = df.loc[(n+1):]
					for i in range( len(df) ):
						addrows(table_name, df.loc[i].to_dict() )
				


























