# Code for ETL operations on Country-GDP data

# Importing the required libraries

import atexit
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from IPython import embed


log_file_path = "./code_log.txt"
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
    resp = requests.get(data_url)

    if resp.status_code != 200:
        raise RuntimeError(f"Failed to GET Data via HTTP!")

    soup = BeautifulSoup(markup=resp.text, features="html.parser")

    return df


def transform(df: pd.DataFrame, csv_path: str) -> pd.DataFrame:
    """This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies"""
    return df


def load_to_csv(df, output_path):
    """This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing."""


def load_to_db(df, sql_connection, table_name):
    """This function saves the final data frame to a database
    table with the provided name. Function returns nothing."""


def run_query(query_statement, sql_connection):
    """This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing."""


""" Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function."""


# variables
data_url = "https://web.archive.org/web/20230908091635"
data_url_wiki = "https://en.wikipedia.org/wiki/List_of_largest_banks"
exchange_rate_csv_path = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
table_attr_extraction = ["Name", "MC_USD_Billion"]
table_attr_final = table_attr_extraction[:] + [
    "MC_GBP_Billion",
    "MC_EUR_Billion",
    "MC_INR_Billion",
]
output_csv_path = "./Largest_banks_data.csv"
database_name = "Banks.db"
database_table_name = "Largest_banks"

log_progress("Preliminaries complete. Initiating ETL process")

df_extracted = extract(url=data_url, table_attribs=table_attr_extraction)
log_progress("Data extraction complete. Initiating Transformation process")

df_transformed = transform(df=df_extracted, csv_path=exchange_rate_csv_path)
log_progress("Data transformation complete. Initiating Loading process")