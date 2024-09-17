
# Code for ETL operations on Country-GDP data
# Importing the required libraries
import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
import sqlite3
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    time_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    log_entry = f"{time_stamp} : {message}\n"
    
    with open('code_log.txt', 'a') as log_file:
        log_file.write(log_entry)
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    tables = soup.find_all('table', class_='wikitable')
    target_table = tables[0] 
    headers = [header.text.strip() for header in target_table.find_all('th')]
    rows = []
    for row in target_table.find_all('tr')[1:]:  
        cols = row.find_all('td')
        if cols:  
            bank_data = [col.text.strip() for col in cols]
            
            if len(bank_data) > 2:  
                bank_data[2] = float(bank_data[2].replace(',', '').replace('€', '').replace('\n', ''))
            rows.append(bank_data)
    df = pd.DataFrame(rows, columns=headers)
    log_progress("Data extraction complete. Initiating Transformation process")
    return df
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = None  # Not used in this implementation
df = extract(url, table_attribs)
def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_rates = pd.read_csv(csv_path)
    exchange_rate_dict = exchange_rates.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate_dict['GBP'], 2) for x in df['Market cap(US$ billion)']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate_dict['EUR'], 2) for x in df['Market cap(US$ billion)']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate_dict['INR'], 2) for x in df['Market cap(US$ billion)']]
    log_progress("Data transformation complete. Initiating Loading process")
    return df
csv_path = 'exchange_rate.csv'
df = transform(df, csv_path)
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)
    log_progress("Data saved to CSV file")
output_path = 'output_file.csv' 
load_to_csv(df, output_path)
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress("Data loaded to Database as a table, Executing queries")
sql_connection = sqlite3.connect('Banks.db')
table_name = 'Largest_banks'
load_to_db(df, sql_connection, table_name)
log_progress("SQL Connection initiated")
def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(f"Executing Query: {query_statement}")
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    results = cursor.fetchall()
    for row in results:
        print(row)
    log_progress("Process Complete")
query1 = "SELECT * FROM Largest_banks"
run_query(query1, sql_connection)
query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query2, sql_connection)
query3 = "SELECT \"Bank name\" FROM Largest_banks LIMIT 5"
run_query(query3, sql_connection)
sql_connection.close()
log_progress("Server Connection closed")
