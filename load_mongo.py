#!/usr/bin/python3
import pandas as pd
from pymongo import MongoClient
from datetime import datetime


client = MongoClient()
db = client.coronavirus
france_covid19 = db.france_covid19

df_confirmed = pd.read_csv("france_coronavirus_time_series-confirmed.csv", sep=";")
df_deaths = pd.read_csv("france_coronavirus_time_series-deaths.csv", sep=";")
df_confirmed = df_confirmed.set_index("Date")
df_deaths = df_deaths.set_index("Date")
df_confirmed["Type"] = "Confirmed"
df_deaths["Type"] = "Death"
df_joined = pd.concat([df_confirmed, df_deaths]).set_index(["Date","Type"]).stack().reset_index()
df_joined["Value"] = df_joined[0]
df_joined["Region"] = df_joined["level_2"]
for row in df_joined.pivot_table(index=["Date","Region"],columns="Type",values="Value").reset_index().to_dict("records"):
    date_value = datetime.strptime(row["Date"], "%d/%m/%Y")
    france_covid19.update({
        "Date": date_value,
        "Region": row["Region"]
    }, {
        "Date": date_value,
        "Region": row["Region"],
        "Confirmed": row["Confirmed"],
        "Death": row["Death"]
    }, upsert=True)
