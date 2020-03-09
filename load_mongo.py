#!/usr/bin/python3
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import numpy as np
import unidecode


client = MongoClient()
db = client.coronavirus
france_covid19 = db.france_covid19

df_confirmed = pd.read_csv("france_coronavirus_time_series-confirmed.csv", sep=";")
df_deaths = pd.read_csv("france_coronavirus_time_series-deaths.csv", sep=";")
df_recovered = pd.read_csv("france_coronavirus_time_series-recovered.csv", sep=";")
df_confirmed["Type"] = "Confirmed"
df_deaths["Type"] = "Death"
df_recovered["Type"] = "Recovered"
df_joined = pd.concat([df_confirmed, df_deaths, df_recovered]).set_index(["Date", "Type"]).stack().reset_index()
df_joined["Value"] = df_joined[0]
df_joined["Region"] = df_joined["level_2"]
df_joined["Region_Code"] = df_joined["Region"].str.upper().apply(unidecode.unidecode)
for row in df_joined.pivot_table(index=["Date", "Region", "Region_Code"], columns="Type", values="Value").replace({np.NAN: None}).reset_index().to_dict("records"):
    date_value = datetime.strptime(row["Date"], "%d/%m/%Y")
    france_covid19.update({
        "Date": date_value,
        "Region": row["Region"]
    }, {
        "Date": date_value,
        "Region": row["Region"],
        "Region_Code": row["Region_Code"],
        "Confirmed": row["Confirmed"],
        "Recovered": row["Recovered"],
        "Death": row["Death"]
    }, upsert=True)
