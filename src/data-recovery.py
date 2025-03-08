import aiohttp
import asyncio
import json
import polars as pl
import os
import boto3
from io import BytesIO
from datetime import datetime
import pytz

# OpenWeather API Key
API_KEY = "b022acb509eacae0875ded1afe41a527"

# URL of the OpenWeather API
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# URL of the REST Countries API
COUNTRIES_API_URL = "https://restcountries.com/v3.1/all?fields=name,capital"

# Mapping dictionary to normalize capital names
CAPITALS_MAPPING = {
    "Papeetē": "Papeete",
    "St. Peter Port": "Saint-Pierre-Port"
}

# =================================================================
#           FUNCTION TO FETCH CAPITALS VIA API
# =================================================================
def normalize_capital_name(capital):
    """
    Normalizes the capital name using the mapping dictionary.
    """
    return CAPITALS_MAPPING.get(capital, capital)

async def fetch_capitals_from_api():
    """
    Fetches the names of capitals and countries from the REST Countries API.
    Excludes entries where the capital is 'No Capital'.
    Normalizes improperly formatted capital names.
    """
    print("[INFO] Fetching capitals via the REST Countries API...")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(COUNTRIES_API_URL) as response:
                if response.status == 200:
                    countries = await response.json()
                    capitals = []
                    for country in countries:
                        name = country.get('name', {}).get('common', 'Unknown Country')
                        capital_list = country.get('capital', [])
                        capital = capital_list[0] if capital_list else 'No Capital'

                        # Normalize capital names
                        capital = normalize_capital_name(capital)

                        # Exclude capitals with the value 'No Capital'
                        if capital != 'No Capital':
                            capitals.append({"country": name, "city": capital})

                    print(f"[SUCCESS] Retrieved {len(capitals)} capitals (excluding 'No Capital').")
                    return capitals
                else:
                    print(f"[ERROR] Error {response.status} when calling the REST Countries API.")
                    return []
        except Exception as e:
            print(f"[ERROR] Error fetching capitals: {e}")
            return []

# =================================================================
#        ASYNC FUNCTIONS TO FETCH WEATHER DATA
# =================================================================
async def fetch_weather_data(session, api_key, city):
    """
    Asynchronously calls the OpenWeather API to fetch weather data for a city.
    """
    params = {"appid": api_key, "lang": "en", "q": city}
    try:
        async with session.get(BASE_URL, params=params) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"[ERROR] Error {response.status} for city '{city}'.")
                return {"error": f"Status {response.status}"}
    except Exception as e:
        print(f"[ERROR] Error calling API for '{city}': {e}")
        return {"error": str(e)}

def fetch_weather_for_all_capitals(api_key, capitals):
    """
    Wrapper to execute the asynchronous call in a synchronous manner.
    """
    return asyncio.run(fetch_weather_for_all_capitals_async(api_key, capitals))

async def fetch_weather_for_all_capitals_async(api_key, capitals):
    """
    Asynchronously fetches weather data for a list of capitals.
    """
    print("[INFO] Starting weather data retrieval for all capitals...")
    error_log = []
    weather_data = {}

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_weather_data(session, api_key, capital["city"]) for capital in capitals]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for capital, result in zip(capitals, results):
            city = capital["city"]
            country = capital["country"]
            if isinstance(result, dict) and "error" not in result:
                print(f"[SUCCESS] Weather data retrieved for city '{city}', country '{country}'.")
                weather_data[city] = result
            else:
                print(f"[ERROR] Data ignored for city '{city}', country '{country}': {result}")
                error_log.append({"city": city, "country": country, "error": result})
                weather_data[city] = {"error": "Error during retrieval"}

    # Create a log file for errors
    log_dir = "src/logs"
    os.makedirs(log_dir, exist_ok=True)
    current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file_path = os.path.join(log_dir, f"errors_data_recovery_{current_date}.log")

    with open(log_file_path, "w", encoding="utf-8") as error_file:
        json.dump(error_log, error_file, indent=4)

    print(f"[INFO] Any errors have been logged in '{log_file_path}'.")
    return weather_data

# =================================================================
#     FUNCTION TO CONVERT DATA INTO A POLARS DATAFRAME
# =================================================================
def convert_weather_data_to_dataframe(weather_data, capitals):
    """
    Structures the weather data into a Polars DataFrame.
    """
    print("[INFO] Converting weather data to Polars DataFrame...")
    structured_data = []

    for capital in capitals:
        city = capital["city"]
        country = capital["country"]
        data = weather_data.get(city, {})

        # Skip entries containing an error
        if "error" in data:
            print(f"[ERROR] Data for city '{city}', country '{country}' ignored (error present).")
            continue

        structured_data.append({
            "country": country,
            "city": city,
            "id": data.get("id"),
            "lon": data.get("coord", {}).get("lon"),
            "lat": data.get("coord", {}).get("lat"),
            "base": data.get("base"),
            "main": data.get("weather", [{}])[0].get("main"),
            "description": data.get("weather", [{}])[0].get("description"),
            "temp": data.get("main", {}).get("temp"),
            "feels_like": data.get("main", {}).get("feels_like"),
            "temp_min": data.get("main", {}).get("temp_min"),
            "temp_max": data.get("main", {}).get("temp_max"),
            "pressure": data.get("main", {}).get("pressure"),
            "humidity": data.get("main", {}).get("humidity"),
            "sea_level": data.get("main", {}).get("sea_level"),
            "grnd_level": data.get("main", {}).get("grnd_level"),
            "visibility": data.get("visibility"),
            "speed": data.get("wind", {}).get("speed"),
            "deg": data.get("wind", {}).get("deg"),
            "clouds": data.get("clouds", {}).get("all"),
            "dt": data.get("dt"),
            "sunrise": data.get("sys", {}).get("sunrise"),
            "sunset": data.get("sys", {}).get("sunset"),
            "timezone": data.get("timezone"),
            "cod": data.get("cod")
        })

    df = pl.DataFrame(structured_data)
    print("[SUCCESS] Polars DataFrame created successfully.")
    return df

# =================================================================
#       FUNCTION TO UPLOAD THE DATAFRAME TO LOCALSTACK S3
# =================================================================
def upload_dataframe_to_s3(df, bucket_name, object_name, endpoint_url="http://localstack-data-lake-project:4566"):
    """
    Uploads a Polars DataFrame directly to a LocalStack S3 bucket
    without saving it locally.
    """
    print("[INFO] Starting upload of DataFrame to S3...")
    csv_buffer = BytesIO()
    csv_content = df.write_csv()
    csv_buffer.write(csv_content.encode("utf-8"))
    csv_buffer.seek(0)

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="test",
        aws_secret_access_key="test"
    )

    try:
        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())
        print(f"[SUCCESS] File '{object_name}' uploaded successfully to bucket '{bucket_name}'.")
    except Exception as e:
        print(f"[ERROR] Error uploading file '{object_name}' to S3: {e}")

# =================================================================
#                   MAIN FUNCTION
# =================================================================
def main():
    print("[INFO] Starting data-recovery script.")

    # Fetch capitals via the REST Countries API
    capitals = asyncio.run(fetch_capitals_from_api())
    if not capitals:
        print("[ERROR] No capitals were retrieved. Exiting program.")
        return

    # Retrieve weather data for all capitals
    all_weather_data = fetch_weather_for_all_capitals(API_KEY, capitals)

    # Convert the collected data into a Polars DataFrame
    df_weather = convert_weather_data_to_dataframe(all_weather_data, capitals)

    # Generate the file name with the current date/time
    paris_tz = pytz.timezone("Europe/Paris")
    current_date = datetime.now(paris_tz).strftime("%Y-%m-%d_%H-%M-%S")
    object_name = f"weather_data_{current_date}.csv"
    bucket_name = "raw"

    # Upload the DataFrame directly to the LocalStack S3 bucket
    upload_dataframe_to_s3(df_weather, bucket_name, object_name)

    print("[SUCCESS] Data-recovery script finished successfully.")

if __name__ == "__main__":
    main()
