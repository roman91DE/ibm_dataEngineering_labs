# Code for ETL operations on Country-GDP data

# Importing the required libraries

import atexit
import sqlite3
from datetime import datetime

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup, Tag
from IPython import embed

log_file_path = "./final_project/code_log.txt"
logfile = open(log_file_path, "a", encoding="utf-8")
atexit.register(lambda: logfile.close())


def log_progress(message: str) -> None:
    """This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing"""
    time_str = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")
    logfile.write(f"{time_str} : {message}\n")


def extract(url: str, table_attribs: list[str]) -> pd.DataFrame:
    """This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing."""
    resp = requests.get(url)

    if resp.status_code != 200:
        raise RuntimeError(f"Failed to GET Data via HTTP!")

    soup = BeautifulSoup(markup=resp.text, features="html.parser")

    tables = soup.find_all("table")  # the table we need is the first one on the page
    table = tables[1]

    if not isinstance(table, Tag):
        raise RuntimeError("Failed to extract Table Tag")

    # extract column headers and row values
    cols = [elem.text.strip() for elem in table.find_all("th")]
    vals = []
    for idx, row in enumerate(table.find_all("tr")):
        if idx == 0:
            continue
        vals.append([val.text.strip() for val in row.find_all("td")])
    df = pd.DataFrame(data=vals, columns=cols).drop(columns="Rank")
    df["Market cap(US$ billion)"] = df["Market cap(US$ billion)"].astype(float)

    return df.rename(columns=dict(zip(df.columns, table_attribs)))


def transform(df: pd.DataFrame, csv_path: str) -> pd.DataFrame:
    """This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies"""

    # get the exchange rates
    resp = requests.get(csv_path)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to GET Data via HTTP!")

    # create the dictionary from the response
    exchange_rates = {}
    for row in resp.text.split()[1:]:
        key, val = row.split(",")
        exchange_rates[key] = float(val)

    # create the new columns
    df["MC_GBP_Billion"] = np.round(df["MC_USD_Billion"] * exchange_rates["GBP"], 2)
    df["MC_EUR_Billion"] = np.round(df["MC_USD_Billion"] * exchange_rates["EUR"], 2)
    df["MC_INR_Billion"] = np.round(df["MC_USD_Billion"] * exchange_rates["INR"], 2)

    return df


def load_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing."""
    df.to_csv(output_path, index=False)


def load_to_db(
    df: pd.DataFrame, sql_connection: sqlite3.Connection, table_name: str
) -> None:
    """This function saves the final data frame to a database
    table with the provided name. Function returns nothing."""
    df.to_sql(name=table_name, con=sql_connection, if_exists="replace")


def run_query(query_statement: str, sql_connection: sqlite3.Connection) -> None:
    """This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing."""

    print(query_statement)
    rows = sql_connection.execute(query_statement).fetchall()
    for row in rows:
        print(row)


""" Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function."""


# variables
data_url_archive = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
data_url = "https://en.wikipedia.org/wiki/List_of_largest_banks"
exchange_rate_csv_path = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
table_attr_extraction = ["Name", "MC_USD_Billion"]
table_attr_final = table_attr_extraction[:] + [
    "MC_GBP_Billion",
    "MC_EUR_Billion",
    "MC_INR_Billion",
]
output_csv_path = "./final_project/Largest_banks_data.csv"
database_name = "./final_project/Banks.db"
database_table_name = "Largest_banks"

log_progress("Preliminaries complete. Initiating ETL process")

df_extracted = extract(url=data_url_archive, table_attribs=table_attr_extraction)
log_progress("Data extraction complete. Initiating Transformation process")

df_transformed = transform(df=df_extracted, csv_path=exchange_rate_csv_path)
print(df_transformed)  # df['MC_EUR_Billion'][4] -> 163.39
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df=df_transformed, output_path=output_csv_path)
log_progress("Data saved to CSV file")

with sqlite3.connect(database=database_name) as conn:
    log_progress("SQL Connection initiated")

    load_to_db(df=df_transformed, sql_connection=conn, table_name=database_table_name)
    log_progress("Data loaded to Database as a table, Executing queries")

    q1 = "SELECT * FROM Largest_banks"
    run_query(query_statement=q1, sql_connection=conn)
    log_progress("Process Complete")

    q2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
    run_query(query_statement=q2, sql_connection=conn)
    log_progress("Process Complete")

    q3 = "SELECT Name from Largest_banks LIMIT 5"
    run_query(query_statement=q3, sql_connection=conn)
    log_progress("Process Complete")

log_progress("Server Connection closed")
