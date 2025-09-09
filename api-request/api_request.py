import requests


#api_key = "d80bcca71c18bd51d7eb896e8b437468"
#api_url = f"http://api.weatherstack.com/current?access_key={api_key}&query=New York"

#def fetch_data():
#    print("Fetching weather data from API ...")
#    try:
#        response = requests.get(api_url)
#        response.raise_for_status()  #check status of response
#        return response.json()
#    except requests.exceptions.RequestException as e:
#        print (f"an error occured {e}" )
#        raise




def mock_fetch_data():
    return {'request': {'type': 'City', 'query': 'New York, United States of America', 'language': 'en', 'unit': 'm'}, 
    'location': {'name': 'New York', 'country': 'United States of America', 'region': 'New York', 'lat': '40.714', 'lon': '-74.006', 'timezone_id': 'America/New_York', 'localtime': '2025-09-02 08:37', 'localtime_epoch': 1756802220, 'utc_offset': '-4.0'}, 
    'current': {'observation_time': '12:37 PM', 'temperature': 18, 'weather_code': 122, 'weather_icons': ['https://cdn.worldweatheronline.com/images/wsymbols01_png_64/wsymbol_0004_black_low_cloud.png'], 'weather_descriptions': ['Overcast'], 'astro': {'sunrise': '06:24 AM', 'sunset': '07:26 PM', 'moonrise': '04:41 PM', 'moonset': '12:23 AM', 'moon_phase': 'Waxing Gibbous', 'moon_illumination': 66}, 'air_quality': {'co': '412.55', 'no2': '16.65', 'o3': '116', 'so2': '7.4', 'pm2_5': '8.695', 'pm10': '9.065', 'us-epa-index': '1', 'gb-defra-index': '1'}, 'wind_speed': 13, 'wind_degree': 51, 'wind_dir': 'NE', 'pressure': 1023, 'precip': 0, 'humidity': 81, 'cloudcover': 100, 'feelslike': 18, 'uv_index': 1, 'visibility': 16, 'is_day': 'yes'}}