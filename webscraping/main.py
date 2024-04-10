import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3

url = "https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films"
db_name = "Movies.db"
table_name = "Top_50"
csv_path = "/home/project/top_50_films.csv"
df = pd.DataFrame(columns=["Average Rank", "Film", "Year"])


resp = requests.get(url)

if resp.status_code != 200:
    print("Failed to GET HTML")


soup = BeautifulSoup(resp.text, "html.parser")

tables = soup.find_all("tbody")

table = tables[0]

data: dict[str, list[str]] = {"Average Rank": [], "Film": [], "Year": []}

for row in table.find_all("tr"):
    vals = row.find_all("td")
    if len(vals) >= 3:
        data["Average Rank"].append(vals[0].text)
        data["Film"].append(vals[1].text)
        data["Year"].append(vals[2].text)


df = pd.DataFrame(data).set_index("Average Rank")
print(df)