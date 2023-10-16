from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attributes = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion:", "MC_INR_Billion"]
output_path = "./Largest_banks_data.csv"
database_name = "Banks.db"
table_name = "Largest_banks"
log_file = "code_log.txt"

def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as file:
        file.write(timestamp + ":" + message +"\n")

def extract(url, table_attributes):
    page = requests.get(url).text
    data = BeautifulSoup(page, "html.parser")
    df = pd.DataFrame (columns= ["Name", "MC_USD_Billion"])
    tables = data.find_all("tbody")
    rows = tables[0].find_all("tr")
    for row in rows:
        col = row.find_all("td")
        if col:
            bank_name = col[1].find_all("a")[1].text
            market_cap = float(col[2].text)
            data_dict = {"Name": bank_name, "MC_USD_Billion": market_cap}
            df1 = pd.DataFrame (data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df):
    mc_gbp_billion = df["MC_USD_Billion"].tolist()
    mc_gbp_billion = [np.round(x*0.8, 2) for x in mc_gbp_billion]
    df["MC_GBP_Billion"] = mc_gbp_billion
    mc_eur_billion = df["MC_USD_Billion"].tolist()
    mc_eur_billion = [np.round(x*0.93, 2) for x in mc_eur_billion]
    df["MC EUR Billion"] = mc_eur_billion
    mc_inr_billion = df["MC_USD_Billion"].tolist ()
    mc_inr_billion = [np.round(x*82.95, 2) for x in mc_inr_billion]
    df["MC IN Billion"] = mc_inr_billion
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attributes)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, output_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)
query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()