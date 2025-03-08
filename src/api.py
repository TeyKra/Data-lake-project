from fastapi import FastAPI, UploadFile, HTTPException, Query
from typing import List
import boto3
from botocore.exceptions import ClientError
import os
from io import BytesIO
from fastapi.staticfiles import StaticFiles
from datetime import datetime
import requests 
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

# =================================================================
#          INITIALIZATION OF THE FASTAPI APPLICATION
# =================================================================
# Create a FastAPI application instance
app = FastAPI()

# =================================================================
#     ADDING CORS MIDDLEWARE FOR CROSS-ORIGIN REQUESTS
# =================================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =================================================================
#            MOUNTING STATIC FILES FOR THE FRONTEND
# =================================================================
app.mount("/frontend", StaticFiles(directory="frontend"), name="frontend")

# =================================================================
#        CONFIGURATION OF LOCALSTACK S3 BUCKETS & AWS CREDENTIALS
# =================================================================
S3_ENDPOINT_URL = "http://localhost:4566"
AWS_ACCESS_KEY_ID = "test"
AWS_SECRET_ACCESS_KEY = "test"
BUCKETS = ["raw", "staging", "curated"]

# OpenWeather API Key (can be loaded from environment variables or a .env file)
OPENWEATHER_API_KEY = "b022acb509eacae0875ded1afe41a527"

# =================================================================
#                  INITIALIZATION OF THE S3 CLIENT
# =================================================================
s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# =================================================================
#               API ENDPOINT TO LIST AVAILABLE BUCKETS
# =================================================================
@app.get("/buckets")
def list_buckets():
    """
    Lists all available S3 buckets.
    
    Retrieves the list of buckets from the S3 client and returns their names.
    """
    try:
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
        return {"buckets": buckets}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving buckets: {e}")

# =================================================================
#              API ENDPOINT TO LIST FILES IN A BUCKET
# =================================================================
@app.get("/files/{bucket_name}")
def list_files(bucket_name: str):
    """
    Lists files contained in a specific S3 bucket.
    
    Parameters:
    - bucket_name: The name of the bucket to list files from.
    
    Returns:
    - A dictionary containing a list of file names.
    """
    if bucket_name not in BUCKETS:
        raise HTTPException(status_code=404, detail="Bucket not found.")

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        files = [obj['Key'] for obj in response.get('Contents', [])] if "Contents" in response else []
        return {"files": files}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving files: {e}")

# =================================================================
#        API ENDPOINT TO DOWNLOAD A FILE FROM A BUCKET
# =================================================================
@app.get("/download/{bucket_name}/{file_name}")
def download_file(bucket_name: str, file_name: str):
    """
    Downloads a file from a specified S3 bucket.
    
    Parameters:
    - bucket_name: The S3 bucket containing the file.
    - file_name: The name of the file to download.
    
    Returns:
    - A streaming response of the file with appropriate MIME type headers.
    """
    if bucket_name not in BUCKETS:
        raise HTTPException(status_code=404, detail="Bucket not found.")

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        file_content = response["Body"].read()

        # Detect the MIME type of the file
        import mimetypes
        mime_type, _ = mimetypes.guess_type(file_name)
        mime_type = mime_type or "application/octet-stream"

        # Return the file as a streaming response with download headers
        return StreamingResponse(
            BytesIO(file_content),
            media_type=mime_type,
            headers={"Content-Disposition": f"attachment; filename={file_name}"}
        )
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Error downloading file: {e}")

# =================================================================
#         API ENDPOINT TO UPLOAD A FILE TO A BUCKET
# =================================================================
@app.post("/upload/{bucket_name}")
def upload_file(bucket_name: str, file: UploadFile):
    """
    Uploads a file to a specified S3 bucket.
    
    Parameters:
    - bucket_name: The S3 bucket where the file will be stored.
    - file: The file object to be uploaded.
    
    Returns:
    - A success message upon successful upload.
    """
    if bucket_name not in BUCKETS:
        raise HTTPException(status_code=404, detail="Bucket not found.")

    try:
        file_content = file.file.read()
        s3_client.put_object(Bucket=bucket_name, Key=file.filename, Body=file_content)
        return {"message": f"File '{file.filename}' successfully uploaded to bucket '{bucket_name}'."}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Error uploading file: {e}")

# =================================================================
#         API ENDPOINT TO DELETE A FILE FROM A BUCKET
# =================================================================
@app.delete("/delete/{bucket_name}/{file_name}")
def delete_file(bucket_name: str, file_name: str):
    """
    Deletes a file from a specified S3 bucket.
    
    Parameters:
    - bucket_name: The S3 bucket containing the file.
    - file_name: The name of the file to delete.
    
    Returns:
    - A confirmation message upon successful deletion.
    """
    if bucket_name not in BUCKETS:
        raise HTTPException(status_code=404, detail="Bucket not found.")

    try:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)
        return {"message": f"File '{file_name}' successfully deleted from bucket '{bucket_name}'."}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Error deleting file: {e}")

# =================================================================
#       API ENDPOINT TO RETRIEVE WEATHER DATA BY COORDINATES
# =================================================================
@app.get("/weather")
def get_weather_by_coordinates(lat: float, lon: float):
    """
    Retrieves weather data based on provided latitude and longitude,
    then stores the data as a CSV file in the 'raw' S3 bucket with
    a timestamped filename.
    
    Parameters:
    - lat: Latitude of the location.
    - lon: Longitude of the location.
    
    Returns:
    - A dictionary containing a success message and the retrieved weather data.
    """
    # Prepare the OpenWeather API call
    weather_url = "https://api.openweathermap.org/data/2.5/weather"
    weather_params = {
        "lat": lat,
        "lon": lon,
        "appid": OPENWEATHER_API_KEY,
        "lang": "fr"
    }

    try:
        # Retrieve weather data from OpenWeather API
        weather_response = requests.get(weather_url, params=weather_params)
        if weather_response.status_code != 200:
            raise HTTPException(
                status_code=weather_response.status_code,
                detail=f"OpenWeather error: {weather_response.json().get('message', 'Unknown error')}"
            )
        weather_data = weather_response.json()

        # Extract country code from weather data
        country_code = weather_data.get("sys", {}).get("country", "N/A")

        # Convert country code to full country name using Rest Countries API
        country_name = "N/A"
        if country_code != "N/A":
            rest_countries_url = f"https://restcountries.com/v3.1/alpha/{country_code}"
            rest_countries_response = requests.get(rest_countries_url)
            if rest_countries_response.status_code == 200:
                country_info = rest_countries_response.json()
                country_name = country_info[0].get("name", {}).get("common", country_code)
            else:
                country_name = country_code  # Fallback to country code if API call fails

        # Build the weather information dictionary
        weather_info = {
            "country": country_name,
            "city": weather_data.get("name", "N/A"),
            "id": weather_data.get("id", 0),
            "lon": weather_data.get("coord", {}).get("lon", lon),
            "lat": weather_data.get("coord", {}).get("lat", lat),
            "base": weather_data.get("base", "N/A"),
            "main": weather_data.get("weather", [{}])[0].get("main", "N/A"),
            "description": weather_data.get("weather", [{}])[0].get("description", "N/A"),
            "temp": weather_data.get("main", {}).get("temp", 0.0),
            "feels_like": weather_data.get("main", {}).get("feels_like", 0.0),
            "temp_min": weather_data.get("main", {}).get("temp_min", 0.0),
            "temp_max": weather_data.get("main", {}).get("temp_max", 0.0),
            "pressure": weather_data.get("main", {}).get("pressure", 0),
            "humidity": weather_data.get("main", {}).get("humidity", 0),
            "sea_level": weather_data.get("main", {}).get("sea_level", 0),
            "grnd_level": weather_data.get("main", {}).get("grnd_level", 0),
            "visibility": weather_data.get("visibility", 0),
            "speed": weather_data.get("wind", {}).get("speed", 0.0),
            "deg": weather_data.get("wind", {}).get("deg", 0),
            "clouds": weather_data.get("clouds", {}).get("all", 0),
            "dt": weather_data.get("dt", 0),
            "sunrise": weather_data.get("sys", {}).get("sunrise", 0),
            "sunset": weather_data.get("sys", {}).get("sunset", 0),
            "timezone": weather_data.get("timezone", 0),
            "cod": weather_data.get("cod", 0)
        }

        # Convert the weather information into CSV format
        csv_header = ",".join(weather_info.keys())
        csv_values = ",".join(str(v) for v in weather_info.values())
        csv_content = f"{csv_header}\n{csv_values}"

        # Generate a filename with the current date and time
        current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"user_input_data_{current_date}.csv"

        # Upload the CSV content to the 'raw' S3 bucket
        try:
            s3_client.put_object(
                Bucket="raw",
                Key=filename,
                Body=csv_content.encode("utf-8")
            )
        except ClientError as e:
            raise HTTPException(status_code=500, detail=f"S3 error during CSV upload: {e}")

        return {
            "message": f"Weather data for lat={lat}, lon={lon} stored as '{filename}'.",
            "weather_data": weather_info
        }

    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Connection error with OpenWeather or Rest Countries API: {str(e)}")

# =================================================================
#                     HEALTH CHECK ENDPOINT
# =================================================================
@app.get("/healthcheck")
def health_check():
    """
    Simple health check endpoint to verify that the API is running.
    
    Returns:
    - A JSON object indicating the API status.
    """
    return {"status": "API is running."}
