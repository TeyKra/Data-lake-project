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

# Initialisation de l'application FastAPI
app = FastAPI()

app.mount("/frontend", StaticFiles(directory="frontend"), name="frontend")

# Configuration des buckets LocalStack
S3_ENDPOINT_URL = "http://localhost:4566"
AWS_ACCESS_KEY_ID = "test"
AWS_SECRET_ACCESS_KEY = "test"
BUCKETS = ["raw", "staging", "curated"]

# Clé API OpenWeather (vous pouvez la récupérer d'un fichier .env, variable d'environnement, etc.)
OPENWEATHER_API_KEY = "b022acb509eacae0875ded1afe41a527"

# Client S3
s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

@app.get("/buckets")
def list_buckets():
    """
    Liste les buckets disponibles.
    """
    try:
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
        return {"buckets": buckets}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la récupération des buckets : {e}")

@app.get("/files/{bucket_name}")
def list_files(bucket_name: str):
    """
    Liste les fichiers dans un bucket spécifique.
    """
    if bucket_name not in BUCKETS:
        raise HTTPException(status_code=404, detail="Bucket non trouvé.")

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        files = [obj['Key'] for obj in response.get('Contents', [])] if "Contents" in response else []
        return {"files": files}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la récupération des fichiers : {e}")

@app.get("/download/{bucket_name}/{file_name}")
def download_file(bucket_name: str, file_name: str):
    """
    Télécharge un fichier depuis un bucket spécifique.
    """
    if bucket_name not in BUCKETS:
        raise HTTPException(status_code=404, detail="Bucket non trouvé.")

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        file_content = response["Body"].read()

        # Détecter le type MIME
        import mimetypes
        mime_type, _ = mimetypes.guess_type(file_name)
        mime_type = mime_type or "application/octet-stream"

        # Retourner une réponse en streaming
        return StreamingResponse(
            BytesIO(file_content),
            media_type=mime_type,
            headers={"Content-Disposition": f"attachment; filename={file_name}"}
        )
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors du téléchargement du fichier : {e}")

@app.post("/upload/{bucket_name}")
def upload_file(bucket_name: str, file: UploadFile):
    """
    Upload un fichier vers un bucket spécifique.
    """
    if bucket_name not in BUCKETS:
        raise HTTPException(status_code=404, detail="Bucket non trouvé.")

    try:
        file_content = file.file.read()
        s3_client.put_object(Bucket=bucket_name, Key=file.filename, Body=file_content)
        return {"message": f"Fichier '{file.filename}' uploadé avec succès dans le bucket '{bucket_name}'."}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de l'upload du fichier : {e}")

@app.delete("/delete/{bucket_name}/{file_name}")
def delete_file(bucket_name: str, file_name: str):
    """
    Supprime un fichier dans un bucket spécifique.
    """
    if bucket_name not in BUCKETS:
        raise HTTPException(status_code=404, detail="Bucket non trouvé.")

    try:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)
        return {"message": f"Fichier '{file_name}' supprimé avec succès du bucket '{bucket_name}'."}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors de la suppression du fichier : {e}")
    

@app.get("/weather")
def get_weather_by_coordinates(lat: float, lon: float):
    """
    Récupère les données météo à partir de la latitude et de la longitude
    fournies par l'utilisateur, puis les stocke au format CSV dans le
    bucket 'raw' sous le nom 'user_input_data_{date_heure}.csv'.
    
    Paramètres:
    - lat: Latitude de la localisation
    - lon: Longitude de la localisation
    
    Retourne:
    - Un dictionnaire contenant toutes les informations météo récupérées.
    """
    # Préparation de l'appel à l'API OpenWeather
    weather_url = "https://api.openweathermap.org/data/2.5/weather"
    weather_params = {
        "lat": lat,
        "lon": lon,
        "appid": OPENWEATHER_API_KEY,
        "lang": "fr"
    }

    try:
        # Récupération des données météo
        weather_response = requests.get(weather_url, params=weather_params)
        if weather_response.status_code != 200:
            raise HTTPException(
                status_code=weather_response.status_code,
                detail=f"Erreur OpenWeather: {weather_response.json().get('message', 'Erreur inconnue')}"
            )
        weather_data = weather_response.json()

        # Récupération du code pays
        country_code = weather_data.get("sys", {}).get("country", "N/A")

        # Conversion du code pays en nom complet avec l'API Rest Countries
        country_name = "N/A"
        if country_code != "N/A":
            rest_countries_url = f"https://restcountries.com/v3.1/alpha/{country_code}"
            rest_countries_response = requests.get(rest_countries_url)
            if rest_countries_response.status_code == 200:
                country_info = rest_countries_response.json()
                country_name = country_info[0].get("name", {}).get("common", country_code)
            else:
                country_name = country_code  # Si l'API échoue, garder le code pays

        # Construction des informations météo
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

        # Conversion en CSV
        csv_header = ",".join(weather_info.keys())
        csv_values = ",".join(str(v) for v in weather_info.values())
        csv_content = f"{csv_header}\n{csv_values}"

        # Nom du fichier de sortie
        current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"user_input_data_{current_date}.csv"

        # Envoi vers S3 (bucket raw)
        try:
            s3_client.put_object(
                Bucket="raw",
                Key=filename,
                Body=csv_content.encode("utf-8")
            )
        except ClientError as e:
            raise HTTPException(status_code=500, detail=f"Erreur S3 lors de l'upload du CSV : {e}")

        return {
            "message": f"Données météo pour lat={lat}, lon={lon} stockées sous '{filename}'.",
            "weather_data": weather_info
        }

    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Erreur de connexion à l'API OpenWeather ou Rest Countries: {str(e)}")


@app.get("/healthcheck")
def health_check():
    """
    Vérifie si l'API est en cours d'exécution.
    """
    return {"status": "API en cours d'exécution."}