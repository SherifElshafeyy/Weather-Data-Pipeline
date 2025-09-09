import psycopg2  # library to connect to db
from api_request import mock_fetch_data


def connect_to_db():  
    print("Connecting to the postgres database")
    try:
        conn = psycopg2.connect(
            host="db",
            port=5432,
            dbname="db",
            user="db_user",
            password="db_password"
        )
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed {e}")
        raise


def create_table(conn):
    print("Creating table if not exists...")
    try:
        cursor = conn.cursor()

        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
        """)

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
        print("Table was created")
    except psycopg2.Error as e:
        print(f"Failed to create table {e}")
        raise


def insert_record(conn, data):
    print("Inserting weather data into database ...")
    try:
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
        print("Data successfully inserted")
    except psycopg2.Error as e:
        print(f"Data not inserted {e}")
        raise


def main():
    try:
        data = mock_fetch_data()
        conn = connect_to_db()
        create_table(conn)
        insert_record(conn, data)
    except Exception as e:
        print(f"An error occurred during execution {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed")
