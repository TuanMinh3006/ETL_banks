# -*- coding: utf-8 -*-
"""
Created on Thu Feb 29 21:16:22 2024

@author: ADMIN
"""

import pandas as pd
from bs4 import BeautifulSoup 
from datetime import datetime
import requests
import sqlite3 

path_log='D:\Python\Bai_Tap_Crouse\Banks\code_log.txt'
url ='https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
output_load='D:\Python\Bai_Tap_Crouse\Banks\Largest_banks_data.csv'
path_db='D:\Python\Bai_Tap_Crouse\Banks\Banks.db'
table_name='Largest_banks'
table_attributes=['Name','MC_USD_Billion']
tables_attributes_1=['Name', 'MC_USD_Billion','MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
rate_path='D:\Python\Bai_Tap_Crouse\Banks\exchange_rate.csv'


def log_progress(message):
    date_format='%Y-%h-%d-%H:%M:%S'
    now=datetime.now()
    time=now.strftime(date_format)
    with open(path_log,'a') as f:
        f.write(time + ' : '+ message+'\n')
        
def extract(url,table_attributes):
    html_page=requests.get(url).text
    data = BeautifulSoup(html_page,'html.parser')
    df= pd.DataFrame(columns=table_attributes)
    tables=data.find_all('tbody')
    rows=tables[0].find_all('tr')
    
    for row in rows:
        cols=row.find_all('td')
        if len(cols) != 0 :
            name=cols[1].find_all('a')
            if len(name)!=0  :                
                data_dict={'Name':name[1].contents[0],
                           'MC_USD_Billion':float(cols[2].contents[0].rstrip("\n"))}
                data_1=pd.DataFrame(data_dict,index=[0])
                df=pd.concat([data_1,df],ignore_index=True)
    print(df)          
    return df

def transform(df,csv_path):
    rate_df=pd.read_csv(csv_path)
    dict = rate_df.set_index('Currency').to_dict()['Rate']
    EUR_rate=float(dict['EUR'])# chuyển df sang dict với(set index) key : Name và value MC_USD_Billion
    print(dict)
    GBP_rate=float(dict['GBP'])
    print(GBP_rate)
    INR_rate=float(dict['INR'])
    print(INR_rate)
    df['MC_GBP_Billion']=round(df['MC_USD_Billion']*GBP_rate,2)
    df['MC_EUR_Billion']=round(df['MC_USD_Billion']*EUR_rate,2)
    df['MC_INR_Billion']=round(df['MC_USD_Billion']*INR_rate,2)
    return df

def load_to_csv(df,output_load):
    df.to_csv(output_load)
    
def load_to_db(df,sql_connection,table_name):
    df.to_sql(table_name,sql_connection,if_exists='replace',index=False)
    
def run_query(query_statement,sql_connection):
    query_out=pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(query_out)




log_progress('Preliminaries complete. Initiating ETL process')


df=extract(url, table_attributes)
log_progress('Data extraction complete. Initiating Transformation process')
print(df)

tranformed_df=transform(df,rate_path)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df, output_load)
log_progress("Data saved to CSV file")

sql_connection=sqlite3.connect(path_db)
log_progress('SQL Connection initiated')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')


query_1= f'SELECT * FROM Largest_banks'
query_2=f'SELECT AVG(MC_GBP_Billion) FROM Largest_banks' 
query_3 =f'SELECT Name FROM Largest_banks LIMIT 5'


run_query(query_1, sql_connection)


run_query(query_2, sql_connection)


run_query(query_3, sql_connection)
log_progress('Process Complete')

sql_connection.close()
log_progress('Server Connection closed')