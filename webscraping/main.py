import logging
import sqlite3
import sys
from os.path import join

import pandas as pd
import requests
from bs4 import BeautifulSoup


def get_logger(name: str, log_path: str) -> logging.Logger:
    logger = logging.Logger(name)
    logger.setLevel(logging.DEBUG)
    file_handler: logging.Handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)
    stream_handler: logging.Handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    for handler in (file_handler, stream_handler):
        logger.addHandler(handler)

    return logger

url = "https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films"
db_name = join("webscraping", "Movies.db")
table_name = "Top_50"
csv_path = join("webscraping", "top_50_films.csv")

getLogger = get_logger("webscraping - top 50 movies", join("webscraping", "log.txt"))

resp = requests.get(url)
getLogger.info(f"Sent GET Request to {resp.request.url}, HTTP-Status: {resp.status_code}")


if resp.status_code != 200:
    getLogger.error("Failed to GET HTML")
    sys.exit(-1)


soup = BeautifulSoup(resp.text, "html.parser")

tables = soup.find_all("tbody")

if len(tables) == 0:
    getLogger.error("Couldnt extract valid table!")
    sys.exit(-2)

table = tables[0]

data: dict[str, list[str]] = {"Average Rank": [], "Film": [], "Year": []}

for idx, row in enumerate(table.find_all("tr")):
    vals = row.find_all("td")
    if len(vals) >= 3:
        data["Average Rank"].append(vals[0].text)
        data["Film"].append(vals[1].text)
        data["Year"].append(vals[2].text)
    if idx == 50:
        break


df = pd.DataFrame(data).set_index("Average Rank")


with open(csv_path, "w") as f:
    df.to_csv(f)
    getLogger.info(f"Written CSV File to {csv_path}")


with sqlite3.connect(db_name) as conn:
    df.to_sql(table_name, conn, if_exists="replace", index=True, index_label="Rank")
    getLogger.info(f"Written Records to Table {table_name} in SQLITE Database {db_name}")
