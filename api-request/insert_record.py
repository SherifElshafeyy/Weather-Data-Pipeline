import requests
import psycopg2
from datetime import datetime, timedelta


api_key = "34a137018d949b4c0372f3b1e578ab34"
api_url = f"http://api.weatherstack.com/current?access_key={api_key}&query=New York"

#function to test availability of api
def is_api_available():
    """Check if API is available (used by PythonSensor)."""
    try:
        response = requests.get(api_url)
        return response.status_code == 200
    except Exception:
        return False


#start fetching data from api
def fetch_data():
    """Fetch weather data from API and return JSON."""
    print("Fetching weather data from API ...")
    response = requests.get(api_url)
    if response.status_code != 200:  # fixed indentation
        raise Exception(f"API request failed with {response.status_code}: {response.text}")
    return response.json()


#mock data for testing (dbt_test succeded , there is bad data)(temeprature=-50)
#def mock_fetch_data():
#    return {'request': {'type': 'City', 'query': 'New York, United States of America', 'language': 'en', 'unit': 'm'}, 
#    'location': {'name':'New York' , 'country': 'United States of America', 'region': 'New York', 'lat': '30.714', 'lon': '-50.006', 'timezone_id': 'America/New_York', 'localtime': '2025-09-03 08:45', 'localtime_epoch': 1756802220, 'utc_offset': '-4.0'}, 
#    'current': {'observation_time': '12:37 PM', 'temperature': -50, 'weather_code': 122, 'weather_icons': ['https://cdn.worldweatheronline.com/images/wsymbols01_png_64/wsymbol_0004_black_low_cloud.png'], 'weather_descriptions': ['Overcast'], 'astro': {'sunrise': '06:24 AM', 'sunset': '07:26 PM', 'moonrise': '04:41 PM', 'moonset': '12:23 AM', 'moon_phase': 'Waxing Gibbous', 'moon_illumination': 66}, 'air_quality': {'co': '412.55', 'no2': '16.65', 'o3': '116', 'so2': '7.4', 'pm2_5': '8.695', 'pm10': '9.065', 'us-epa-index': '1', 'gb-defra-index': '1'}, 'wind_speed': 13, 'wind_degree': 51, 'wind_dir': 'sherif', 'pressure': 1023, 'precip': 0, 'humidity': 81, 'cloudcover': 100, 'feelslike': 18, 'uv_index': 1, 'visibility': 16, 'is_day': 'yes'}}

#mock data for testing (dbt_test is failed , data is good)
#def mock_fetch_data():
#    return {'request': {'type': 'City', 'query': 'New York, United States of America', 'language': 'en', 'unit': 'm'}, 
#    'location': {'name': 'New York', 'country': 'United States of America', 'region': 'New York', 'lat': '30.714', 'lon': '-50.006', 'timezone_id': 'America/New_York', 'localtime': '2025-09-02 08:37', 'localtime_epoch': 1756802220, 'utc_offset': '-4.0'}, 
#    'current': {'observation_time': '12:37 PM', 'temperature': 25, 'weather_code': 122, 'weather_icons': ['https://cdn.worldweatheronline.com/images/wsymbols01_png_64/wsymbol_0004_black_low_cloud.png'], 'weather_descriptions': ['Overcast'], 'astro': {'sunrise': '06:24 AM', 'sunset': '07:26 PM', 'moonrise': '04:41 PM', 'moonset': '12:23 AM', 'moon_phase': 'Waxing Gibbous', 'moon_illumination': 66}, 'air_quality': {'co': '412.55', 'no2': '16.65', 'o3': '116', 'so2': '7.4', 'pm2_5': '8.695', 'pm10': '9.065', 'us-epa-index': '1', 'gb-defra-index': '1'}, 'wind_speed': 13, 'wind_degree': 51, 'wind_dir': 'NE', 'pressure': 1023, 'precip': 0, 'humidity': 81, 'cloudcover': 100, 'feelslike': 18, 'uv_index': 1, 'visibility': 16, 'is_day': 'yes'}}

#connecting to db
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

#create source table
def create_table(conn):
    """Create schema and table if not exists, with run_id column."""
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
            utc_offset TEXT,
            run_id TEXT
        );
    """)
    conn.commit()
    print("Table was created (if not exists).")

#inserting fetched data from source
def insert_record(conn, data, run_id):
    """Insert weather data into Postgres with run_id."""
    location = data['location']
    weather = data['current']
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO dev.raw_weather_data (
            city, country, region, latitude, longitude,
            temperature, feelslike, weather_description,
            wind_speed, wind_direction, humidity,
            visibility, pressure, time, inserted_at, utc_offset, run_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s)
    """, (
        location['name'], location['country'], location['region'],
        float(location['lat']), float(location['lon']),
        weather['temperature'], weather['feelslike'],
        weather['weather_descriptions'][0],
        weather['wind_speed'], weather['wind_dir'],
        weather['humidity'], weather['visibility'],
        weather['pressure'], location['localtime'],
        location['utc_offset'], run_id
    ))
    conn.commit()
    print(f"Data successfully inserted with run_id={run_id}")


#orcherastrator 
def main(run_id=None):
    """Orchestration function: fetch, create table, insert record."""
    try:
        data = fetch_data()
        conn = connect_to_db()
        create_table(conn)
        insert_record(conn, data, run_id)
    except Exception as e:
        print(f"An error occurred during execution: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")

#cleaning up bad recprds from cleansed table in case dbt_test is succeded
def cleanup_failed_run(run_id):
    """Delete cleansed rows for the current run_id if dbt tests fail."""
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM dev.weather_data_cleansed WHERE run_id = %s;", (run_id,))
        cursor.execute("DELETE FROM dev.raw_weather_data WHERE run_id = %s;", (run_id,))
        conn.commit()
        print(f"Cleaned up cleansed data for failed run_id={run_id}")
    except Exception as e:
        print(f"Error during cleanup for run_id={run_id}: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


    