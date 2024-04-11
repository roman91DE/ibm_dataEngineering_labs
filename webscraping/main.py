import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import sys
from os.path import join
from logger.logger import get_logger

url = "https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films"
db_name = join("webscraping", "Movies.db")
table_name = "Top_50"
csv_path = join("webscraping", "top_50_films.csv")

logger = get_logger("webscraping - top 50 movies", join("webscraping", "log.txt"))

resp = requests.get(url)
logger.info(f"Sent GET Request to {resp.request.url}, HTTP-Status: {resp.status_code}")


if resp.status_code != 200:
    logger.error("Failed to GET HTML")
    sys.exit(-1)


soup = BeautifulSoup(resp.text, "html.parser")

tables = soup.find_all("tbody")

if len(tables) == 0:
    logger.error("Couldnt extract valid table!")
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
    logger.info(f"Written CSV File to {csv_path}")


with sqlite3.connect(db_name) as conn:
    df.to_sql(table_name, conn, if_exists="replace", index=True, index_label="Rank")
    logger.info(f"Written Records to Table {table_name} in SQLITE Database {db_name}")
