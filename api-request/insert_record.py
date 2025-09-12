import requests
import psycopg2
from datetime import datetime, timedelta


api_key = "d80bcca71c18bd51d7eb896e8b437468"
api_url = f"http://api.weatherstack.com/current?access_key={api_key}&query=New York"


#def is_api_available():
#    """Check if API is available (used by PythonSensor)."""
#    try:
#        response = requests.get(api_url)
#        return response.status_code == 200
#    except Exception:
#        return False


#def fetch_data():
#    """Fetch weather data from API and return JSON."""
#    print("Fetching weather data from API ...")
#    response = requests.get(api_url)
#    if response.status_code != 200:
#        raise Exception("Failed to fetch data from API")
#    return response.json()


def mock_fetch_data():
    return {'request': {'type': 'City', 'query': 'New York, United States of America', 'language': 'en', 'unit': 'm'}, 
    'location': {'name': 'New York', 'country': 'United States of America', 'region': 'New York', 'lat': '40.714', 'lon': '-74.006', 'timezone_id': 'America/New_York', 'localtime': '2025-09-02 08:37', 'localtime_epoch': 1756802220, 'utc_offset': '-4.0'}, 
    'current': {'observation_time': '12:37 PM', 'temperature': 18, 'weather_code': 122, 'weather_icons': ['https://cdn.worldweatheronline.com/images/wsymbols01_png_64/wsymbol_0004_black_low_cloud.png'], 'weather_descriptions': ['Overcast'], 'astro': {'sunrise': '06:24 AM', 'sunset': '07:26 PM', 'moonrise': '04:41 PM', 'moonset': '12:23 AM', 'moon_phase': 'Waxing Gibbous', 'moon_illumination': 66}, 'air_quality': {'co': '412.55', 'no2': '16.65', 'o3': '116', 'so2': '7.4', 'pm2_5': '8.695', 'pm10': '9.065', 'us-epa-index': '1', 'gb-defra-index': '1'}, 'wind_speed': 13, 'wind_degree': 51, 'wind_dir': 'NE', 'pressure': 1023, 'precip': 0, 'humidity': 81, 'cloudcover': 100, 'feelslike': 18, 'uv_index': 1, 'visibility': 16, 'is_day': 'yes'}}


def connect_to_db():
    """Connect to Postgres database."""
    print("Connecting to the postgres database")
    return psycopg2.connect(
        host="db",
        port=5432,
        dbname="db",
        user="db_user",
        password="db_password"
    )


def create_table(conn):
    """Create schema and table if not exists."""
    cursor = conn.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS dev;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dev.raw_weather_data (
            id SERIAL PRIMARY KEY,
            city TEXT,
            country TEXT,
            region TEXT,
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            feelslike FLOAT,
            weather_description TEXT,
            wind_speed FLOAT,
            wind_direction TEXT,
            humidity FLOAT,
            visibility FLOAT,
            pressure FLOAT,
            time TIMESTAMP,
            inserted_at TIMESTAMP DEFAULT NOW(),
            utc_offset TEXT
        );
    """)
    conn.commit()
    print("Table was created (if not exists).")


def insert_record(conn, data):
    """Insert weather data into Postgres."""
    location = data['location']
    weather = data['current']
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO dev.raw_weather_data (
            city,
            country,
            region,
            latitude,
            longitude,
            temperature,
            feelslike,
            weather_description,
            wind_speed,
            wind_direction,
            humidity,
            visibility,
            pressure,
            time,
            inserted_at,
            utc_offset
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
    """, (
        location['name'],
        location['country'],
        location['region'],
        float(location['lat']),
        float(location['lon']),
        weather['temperature'],
        weather['feelslike'],
        weather['weather_descriptions'][0],
        weather['wind_speed'],
        weather['wind_dir'],
        weather['humidity'],
        weather['visibility'],
        weather['pressure'],
        location['localtime'],
        location['utc_offset']
    ))
    conn.commit()
    print("Data successfully inserted.")


def main():
    """Orchestration function: fetch, create table, insert record."""
    try:
        data = mock_fetch_data()
        conn = connect_to_db()
        create_table(conn)
        insert_record(conn, data)
    except Exception as e:
        print(f"An error occurred during execution: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")