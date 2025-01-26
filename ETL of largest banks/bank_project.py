# Code for ETL operations on Country-GDP data
import pandas as pd 
import requests
from bs4 import BeautifulSoup
import numpy as np 
from datetime import datetime
import sqlite3

url='https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path='exchange_rate.csv'
table_attributes_extract=['Name','MC_USD_Billion']
table_attributes_final=['Name','MC_USD_Billion','MC_GBP_Billion','MC_EUR_Billion','MC_INR_Billion']
output_path='Largest_banks_data.csv'
db_name='Banks.db'
table_name='Largest_banks'
log_file='code_log.txt'



def extract(url, table_attributes_extract):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    data= requests.get(url).text
  
    
    df=pd.DataFrame(columns=table_attributes_extract)
    soup_data=BeautifulSoup(data,'html.parser')
    tables=soup_data.find_all('tbody')
    rows=tables[0].find_all('tr')
    #print(rows)
    for row in rows:
        #print(row)
        col=row.find_all('td')
        #print(col)
        if(len(col)!=0):
            bank_name=col[1].find_all('a')[1]['title']
            market_cap=col[2].contents[0][:-1]
            data_dict={'Name':bank_name,
                    'MC_USD_Billion':float(market_cap)}
            
            df1=pd.DataFrame(data_dict,index=[0])
            df=pd.concat([df,df1],ignore_index=True)
    #print(tables[0])
    #print(df)
    return df







  

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    csvfile = pd.read_csv(csv_path)
   

    dict = csvfile.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = [np.round(x * dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * dict['INR'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * dict['EUR'],2) for x in df['MC_USD_Billion']]

    print(df)
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    for query in query_statement:
        print(query)
        print(pd.read_sql(query, sql_connection), '\n')

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format='%Y-%h-%d-%H:%M:%S'
    now= datetime.now()
    timestamp=now.strftime(timestamp_format)
    with open(log_file,'a') as f:
        f.write(timestamp + ':'+ message +'\n')



log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attributes_extract)
log_progress('Data extraction complete. Initiating Transformation process')



df = transform(df,csv_path)


log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, output_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')

log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')


query_statement = [
        'SELECT * FROM Largest_banks',
        'SELECT AVG(MC_GBP_Billion) FROM Largest_banks',
        'SELECT Name from Largest_banks LIMIT 5'
    ]
run_query(query_statement,sql_connection)

log_progress('Process Complete.')

sql_connection.close()




