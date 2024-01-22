import json
import sqlite3
import requests
from datetime import datetime
#from dateutil import tz
import os
from pathlib import Path

os.chdir(Path(__file__).parent)

# Function to fetch weather data and save to JSON and SQLite
def fetch_and_save_weather(city, country, api_key):
    url = "https://api.openweathermap.org/data/2.5/weather"
    
    # Fetch weather data in German (DE)
    params_de = {
        "q": f"{city},{country}",
        "appid": api_key,
        "units": "metric",
        "lang": "de",
    }
    response_de = requests.get(url, params=params_de)
    response_data_de = response_de.json()

    # Fetch weather data in English (EN)
    params_en = {
        "q": f"{city},{country}",
        "appid": api_key,
        "units": "metric",
        "lang": "en",
    }
    response_en = requests.get(url, params=params_en)
    response_data_en = response_en.json()

    # Create a JSON filename using cityname and current datetime
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    json_filename_de = f"{city}_DE_{current_datetime}.json"
    json_filename_en = f"{city}_EN_{current_datetime}.json"

    # Save the JSON response to local files for both languages
    with open(f'./data/{json_filename_de}', "w") as json_file_de:
        json.dump(response_data_de, json_file_de, indent=4)
    
    with open(f'./data/{json_filename_en}', "w") as json_file_en:
        json.dump(response_data_en, json_file_en, indent=4)

    # Convert UTC time to human-readable time for sunrise and sunset
    sunrise = datetime.utcfromtimestamp(response_data_de["sys"]["sunrise"])
    sunset = datetime.utcfromtimestamp(response_data_de["sys"]["sunset"])

    # Open SQLite database connection
    db_name = 'weather_1'
    conn = sqlite3.connect(f"./DBS/{db_name}.db")
    cursor = conn.cursor()

    # Create the City table if it does not exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS City (
            city_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_name VARCHAR(20) UNIQUE
        )
    """)

    # Check if the city already exists in the City table
    cursor.execute("SELECT city_id FROM City WHERE city_name=?", (city,))
    row = cursor.fetchone()

    if row is None:
        # City does not exist, insert it
        cursor.execute(
            "INSERT INTO City (city_name) VALUES (?)",
            (city,)
        )

        # Get the ID of the newly inserted city
        cursor.execute("SELECT last_insert_rowid()")
        city_id = cursor.fetchone()[0]
    else:
        # City already exists, use its ID
        city_id = row[0]

    # Create the Weather table if it does not exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Weather (
            weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id INT,
            temp DOUBLE,
            temp_max DOUBLE,
            temp_min DOUBLE,
            description_de VARCHAR(20),
            description_en VARCHAR(20),
            sunrise DATE,
            sunset DATE,
            FOREIGN KEY (city_id) REFERENCES City (city_id)
        )
    """)

    # Insert data into the Weather table for German (DE)
    cursor.execute(
        "INSERT INTO Weather (city_id, temp, temp_max, temp_min, description_de, description_en, sunrise, sunset) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            city_id,
            response_data_de["main"]["temp"],
            response_data_de["main"]["temp_max"],
            response_data_de["main"]["temp_min"],
            response_data_de["weather"][0]["description"],
            response_data_en["weather"][0]["description"],
            sunrise.strftime("%Y-%m-%d %H:%M:%S"),
            sunset.strftime("%Y-%m-%d %H:%M:%S"),
        )
    )

    # Insert data into the Weather table for English (EN)
    cursor.execute(
        "INSERT INTO Weather (city_id, temp, temp_max, temp_min, description_de, description_en, sunrise, sunset) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            city_id,
            response_data_en["main"]["temp"],
            response_data_en["main"]["temp_max"],
            response_data_en["main"]["temp_min"],
            response_data_de["weather"][0]["description"],
            response_data_en["weather"][0]["description"],
            sunrise.strftime("%Y-%m-%d %H:%M:%S"),
            sunset.strftime("%Y-%m-%d %H:%M:%S"),
        )
    )

    # Commit changes and close the database connection
    conn.commit()
    conn.close()

def main():
    api_key = "8b2ce810e05f758a07c3309b45bb96aa" 
    cities = [
        {"name": "Berlin", "country": "DE"},
        {"name": "Stuttgart", "country": "DE"},
        {"name": "Bonn", "country": "DE"}
        # ,{"name": "Hamburg", "country": "DE"},
        # {"name": "Kiel", "country": "DE"},
        # {"name": "Koeln", "country": "DE"},
    ]

    for city_info in cities:
        city_name = city_info["name"]
        city_country = city_info["country"]
        fetch_and_save_weather(city_name, city_country, api_key)

if __name__ == "__main__":
    main()
