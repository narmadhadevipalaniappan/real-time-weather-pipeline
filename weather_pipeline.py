import requests
import pandas as pd
import pyodbc
from datetime import datetime

# API URL
url = "https://api.open-meteo.com/v1/forecast?latitude=53.38&longitude=-1.47&current_weather=true"

# Get data
response = requests.get(url)
data = response.json()

weather = data['current_weather']

df = pd.DataFrame([weather])

# 🔥 FIX: Convert time to datetime
df['time'] = pd.to_datetime(df['time'])
df['collected_at'] = pd.Timestamp.now()

# Connect to SQL Server
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost\\SQLEXPRESS01;'
    'DATABASE=LiveWeatherDB;'
    'Trusted_Connection=yes;'
)

cursor = conn.cursor()

# Insert data
for index, row in df.iterrows():
    cursor.execute("""
    INSERT INTO weather (temperature, windspeed, winddirection, time, collected_at)
    VALUES (?, ?, ?, ?, ?)
""", row['temperature'], row['windspeed'], row['winddirection'], row['time'], row['collected_at'])

conn.commit()

print("Data inserted successfully!")