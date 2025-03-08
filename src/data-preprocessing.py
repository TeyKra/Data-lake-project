import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from numba import njit, prange

# =================================================================
#                    NUMBA COMPILED FUNCTIONS
# =================================================================

@njit
def get_season_num(day_of_year, lat):
    """
    Returns an integer corresponding to the season.
    0 -> Spring / 1 -> Summer / 2 -> Autumn / 3 -> Winter
    """
    if lat > 0:
        # Northern Hemisphere
        if 80 <= day_of_year < 172:
            return 0  # Spring
        elif 172 <= day_of_year < 264:
            return 1  # Summer
        elif 264 <= day_of_year < 355:
            return 2  # Autumn
        else:
            return 3  # Winter
    else:
        # Southern Hemisphere
        if 80 <= day_of_year < 172:
            return 2  # Autumn
        elif 172 <= day_of_year < 264:
            return 3  # Winter
        elif 264 <= day_of_year < 355:
            return 0  # Spring
        else:
            return 1  # Summer

@njit
def categorize_temperature_num(temp):
    """
    Returns an integer corresponding to the temperature category.
    0 -> Very Cold / 1 -> Cold / 2 -> Moderate / 3 -> Hot
    """
    if temp < 0:
        return 0
    elif temp < 10:
        return 1
    elif temp < 25:
        return 2
    else:
        return 3

@njit(parallel=True)
def compute_season_and_temp_category(day_of_year_arr, lat_arr, temp_arr):
    """
    Calculates for each row the season and temperature category
    based on the day_of_year, latitude, and temperature.
    Returns two arrays of integers.
    """
    n = len(day_of_year_arr)
    season_codes = [0] * n
    temp_cat_codes = [0] * n
    for i in prange(n):
        season_codes[i] = get_season_num(day_of_year_arr[i], lat_arr[i])
        temp_cat_codes[i] = categorize_temperature_num(temp_arr[i])
    return season_codes, temp_cat_codes

def map_season_code_to_str(code):
    mapping = ["Spring", "Summer", "Autumn", "Winter"]
    return mapping[code]

def map_temp_cat_code_to_str(code):
    mapping = ["Very Cold", "Cold", "Moderate", "Hot"]
    return mapping[code]

# =================================================================
#                S3 FUNCTIONS
# =================================================================

def list_all_files_in_s3(bucket_name, prefix, s3_client):
    """
    Retrieves the list of all files in an S3 bucket 
    matching the specified prefix.
    """
    files = []
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" not in response:
            print(f"[INFO] No file found with prefix '{prefix}' in bucket '{bucket_name}'.")
            return files

        for obj in response["Contents"]:
            files.append(obj["Key"])

        print(f"[SUCCESS] Files found with prefix '{prefix}': {files}")
    except Exception as e:
        print(f"[ERROR] Error retrieving files from S3: {e}")
    return files

def download_file_from_s3(bucket_name, object_name, s3_client):
    """
    Downloads a CSV file from S3 and loads it into a Pandas DataFrame.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
        csv_content = response["Body"].read()
        print(f"[SUCCESS] File '{object_name}' downloaded from S3.")
        return pd.read_csv(BytesIO(csv_content))
    except Exception as e:
        print(f"[ERROR] Error downloading file '{object_name}' from S3: {e}")
        return None

def upload_dataframe_to_s3(df, bucket_name, object_name, s3_client):
    """
    Converts a DataFrame to a CSV file and uploads it directly to S3.
    """
    try:
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())
        print(f"[SUCCESS] File '{object_name}' uploaded successfully to bucket '{bucket_name}'.")
    except Exception as e:
        print(f"[ERROR] Error uploading file '{object_name}' to S3: {e}")

# =================================================================
#                PREPROCESSING FUNCTION
# =================================================================

def preprocess_data(df):
    print("[INFO] Starting data preprocessing...")

    # -------------------------------------------------------------------------
    # 1. REMOVING DUPLICATES
    # -------------------------------------------------------------------------
    duplicates = df[df.duplicated()]
    if not duplicates.empty:
        print(f"[INFO] Duplicates found and removed: {len(duplicates)}")
    df = df.drop_duplicates()
    print("[SUCCESS] Duplicate removal completed.")

    # -------------------------------------------------------------------------
    # 2. REMOVE ROWS WHERE 'cod' != 200
    # -------------------------------------------------------------------------
    if "cod" in df.columns:
        rows_to_delete = df[df["cod"] != 200]
        if not rows_to_delete.empty:
            print(f"[INFO] Rows with 'cod' != 200 removed: {len(rows_to_delete)}")
        df = df[df["cod"] == 200]
        print("[SUCCESS] Removal of rows with 'cod' != 200 completed.")
    else:
        print("[INFO] 'cod' column not present, no removal based on 'cod'.")

    # -------------------------------------------------------------------------
    # 3. ADDING THE 'local_datetime' COLUMN
    # -------------------------------------------------------------------------
    # Check for the presence of "dt" and "timezone" columns
    if "dt" in df.columns and "timezone" in df.columns:
        df["local_datetime"] = pd.to_datetime(df["dt"], unit="s") + pd.to_timedelta(df["timezone"], unit="s")
        print("[SUCCESS] 'local_datetime' column added.")
    else:
        print("[INFO] Unable to calculate 'local_datetime': missing 'dt' or 'timezone' columns.")

    # -------------------------------------------------------------------------
    # 4. REMOVING UNUSED COLUMNS
    # -------------------------------------------------------------------------
    columns_to_drop = ["dt", "timezone", "id", "base", "cod"]
    existing_to_drop = [col for col in columns_to_drop if col in df.columns]
    df.drop(columns=existing_to_drop, inplace=True, errors='ignore')
    print(f"[SUCCESS] Removed columns (if present): {existing_to_drop}")

    # -------------------------------------------------------------------------
    # 5. CONVERTING UNIX TIMESTAMPS TO DATETIME FORMAT
    # -------------------------------------------------------------------------
    for col in ["sunrise", "sunset"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit="s")
    print("[SUCCESS] UNIX timestamp conversion completed for 'sunrise' and 'sunset' (if present).")

    # -------------------------------------------------------------------------
    # 6. CONVERTING TEMPERATURES FROM KELVIN TO CELSIUS
    # -------------------------------------------------------------------------
    temperature_columns = ["temp", "feels_like", "temp_min", "temp_max"]
    for col in temperature_columns:
        if col in df.columns:
            df[col] = df[col] - 273.15
    print("[SUCCESS] Temperature conversion from Kelvin to Celsius completed (if columns present).")

    # -------------------------------------------------------------------------
    # 7. ADDING DAYLIGHT DURATION (DAYLIGHT_DURATION)
    # -------------------------------------------------------------------------
    if "sunrise" in df.columns and "sunset" in df.columns:
        df["daylight_duration"] = (df["sunset"] - df["sunrise"]).dt.total_seconds() / 3600
        print("[SUCCESS] 'daylight_duration' column added.")
    else:
        print("[INFO] Unable to calculate 'daylight_duration': missing 'sunrise' or 'sunset' columns.")

    # -------------------------------------------------------------------------
    # 8. CALCULATING TEMPERATURE DIFFERENCE
    # -------------------------------------------------------------------------
    if "temp_min" in df.columns and "temp_max" in df.columns:
        df["temperature_difference"] = df["temp_max"] - df["temp_min"]
        print("[SUCCESS] 'temperature_difference' column added.")
    else:
        print("[INFO] Unable to calculate 'temperature_difference': missing 'temp_min' or 'temp_max' columns.")

    # -------------------------------------------------------------------------
    # 9. CALCULATING THERMAL COMFORT INDEX (THERMAL_COMFORT_INDEX)
    # -------------------------------------------------------------------------
    if "temp" in df.columns and "humidity" in df.columns and "speed" in df.columns:
        df["thermal_comfort_index"] = (
            df["temp"] - (0.55 * (1 - (df["humidity"] / 100)) * (df["temp"] - 14.5)) - (0.2 * df["speed"])
        )
        print("[SUCCESS] 'thermal_comfort_index' column added.")
    else:
        print("[INFO] Unable to calculate 'thermal_comfort_index': missing 'temp', 'humidity', or 'speed' columns.")

    # -------------------------------------------------------------------------
    # 10. BULK CALCULATION OF 'season' AND 'temperature_category' VIA NUMBA
    # -------------------------------------------------------------------------
    if "local_datetime" in df.columns and "lat" in df.columns and "temp" in df.columns:
        df["day_of_year"] = df["local_datetime"].dt.dayofyear

        # Retrieve the necessary arrays
        day_of_year_arr = df["day_of_year"].values
        lat_arr         = df["lat"].values
        temp_arr        = df["temp"].values

        # Calculation via Numba
        season_codes, temp_cat_codes = compute_season_and_temp_category(day_of_year_arr, lat_arr, temp_arr)

        # Mapping codes to strings
        df["season"] = [map_season_code_to_str(code) for code in season_codes]
        df["temperature_category"] = [map_temp_cat_code_to_str(code) for code in temp_cat_codes]

        # Optionally drop the 'day_of_year' column if not needed
        df.drop(columns=["day_of_year"], inplace=True)
        print("[SUCCESS] 'season' and 'temperature_category' columns added (via Numba).")
    else:
        print("[INFO] Unable to calculate 'season' and 'temperature_category': missing 'local_datetime', 'lat', or 'temp' columns.")

    # -------------------------------------------------------------------------
    # 11. RENAMING COLUMNS
    # -------------------------------------------------------------------------
    rename_columns = {
        "lon": "longitude",
        "lat": "latitude",
        "main": "weather_Condition",
        "description": "weather_description",
        "temp": "temperature",
        "feels_like": "feels_like_temperature",
        "temp_min": "min_temperature",
        "temp_max": "max_temperature",
        "sea_level": "sea_level_pressure",
        "grnd_level": "ground_level_pressure",
        "speed": "wind_Speed",
        "deg": "wind_direction",
        "clouds": "cloud_cover",
        "sunrise": "sunrise_time",
        "sunset": "sunset_time",
    }
    df.rename(columns=rename_columns, inplace=True)
    print("[SUCCESS] Columns renaming completed.")

    # -------------------------------------------------------------------------
    # 12. REORGANIZING COLUMNS
    # -------------------------------------------------------------------------
    column_order = [
        "country", "city", "longitude", "latitude", "weather_Condition", "weather_description",
        "temperature", "temperature_category", "feels_like_temperature", "min_temperature", "max_temperature",
        "temperature_difference", "thermal_comfort_index", "pressure", "sea_level_pressure",
        "ground_level_pressure", "humidity", "visibility", "wind_Speed", "wind_direction", "cloud_cover",
        "sunrise_time", "sunset_time", "daylight_duration", "local_datetime", "season"
    ]
    # Ensure all columns exist (in case some are missing in the data)
    column_order_existing = [col for col in column_order if col in df.columns]
    df = df[column_order_existing]
    print("[SUCCESS] Columns reorganized.")

    # -------------------------------------------------------------------------
    # 13. SORTING DATA ALPHABETICALLY BY 'country'
    # -------------------------------------------------------------------------
    df = df.sort_values(by="country").reset_index(drop=True)
    print("[SUCCESS] Data sorted alphabetically by 'country'.")

    print("[SUCCESS] Data preprocessing completed.")
    return df


# =================================================================
#                       MAIN FUNCTION
# =================================================================

def main():
    bucket_name = "raw"
    staging_bucket = "staging"
    endpoint_url = "http://localstack-data-lake-project:4566"

    # Prefixes to fetch
    prefixes_to_fetch = ["weather_data_", "user_input_data_"]

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="test",
        aws_secret_access_key="test"
    )

    # Retrieve the list of all files for each prefix
    all_files = []
    for prefix in prefixes_to_fetch:
        files_list = list_all_files_in_s3(bucket_name, prefix, s3_client)
        all_files.extend(files_list)  # Add to the global list

    # Ensure at least one file was retrieved
    if not all_files:
        print("[INFO] No file found with the requested prefixes. Exiting program.")
        return

    # Download all files and merge them into a single DataFrame
    list_df = []
    for file_key in all_files:
        df_weather = download_file_from_s3(bucket_name, file_key, s3_client)
        if df_weather is not None:
            list_df.append(df_weather)

    if not list_df:
        print("[INFO] Unable to create a DataFrame from the downloaded files. Exiting program.")
        return

    # Concatenate all DataFrames into one
    merged_df = pd.concat(list_df, ignore_index=True)

    # Preprocess the data
    merged_df = preprocess_data(merged_df)

    # Output file name
    output_file_name = "global_weather_data.csv"

    # Upload the final DataFrame with a fixed name to the staging bucket
    upload_dataframe_to_s3(merged_df, staging_bucket, output_file_name, s3_client)


if __name__ == "__main__":
    main()
