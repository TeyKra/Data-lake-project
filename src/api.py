from fastapi import FastAPI, UploadFile, HTTPException, Query
from typing import List
import boto3
from botocore.exceptions import ClientError
import os
from io import BytesIO
from fastapi.staticfiles import StaticFiles
from datetime import datetime
import requests 

# Initialisation de l'application FastAPI
app = FastAPI()

app.mount("/frontend", StaticFiles(directory="frontend"), name="frontend")

# Configuration des buckets LocalStack
S3_ENDPOINT_URL = "http://localhost:4566"
AWS_ACCESS_KEY_ID = "root"
AWS_SECRET_ACCESS_KEY = "root"
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
        return {"file_name": file_name, "content": file_content.decode("utf-8")}
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
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": OPENWEATHER_API_KEY,
        "lang": "fr"
    }

    try:
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Erreur OpenWeather: {response.json().get('message', 'Erreur inconnue')}"
            )
        data = response.json()

        # Construction d'un dictionnaire respectant les mêmes colonnes que dans data-recovery.py
        # Certaines valeurs peuvent être absentes, on gère par .get()
        weather_info = {
            "country": data.get("sys", {}).get("country", "N/A"),
            "city": data.get("name", "N/A"),
            "id": data.get("id", 0),
            "lon": data.get("coord", {}).get("lon", lon),
            "lat": data.get("coord", {}).get("lat", lat),
            "base": data.get("base", "N/A"),
            "main": data.get("weather", [{}])[0].get("main", "N/A"),
            "description": data.get("weather", [{}])[0].get("description", "N/A"),
            "temp": data.get("main", {}).get("temp", 0.0),
            "feels_like": data.get("main", {}).get("feels_like", 0.0),
            "temp_min": data.get("main", {}).get("temp_min", 0.0),
            "tem_max": data.get("main", {}).get("temp_max", 0.0),  # conforme au DataFrame initial
            "pressure": data.get("main", {}).get("pressure", 0),
            "humidity": data.get("main", {}).get("humidity", 0),
            "sea_level": data.get("main", {}).get("sea_level", 0),
            "grnd_level": data.get("main", {}).get("grnd_level", 0),
            "visibility": data.get("visibility", 0),
            "speed": data.get("wind", {}).get("speed", 0.0),
            "deg": data.get("wind", {}).get("deg", 0),
            "clouds": data.get("clouds", {}).get("all", 0),
            "dt": data.get("dt", 0),
            "sunrise": data.get("sys", {}).get("sunrise", 0),
            "sunset": data.get("sys", {}).get("sunset", 0),
            "timezone": data.get("timezone", 0),
            "cod": data.get("cod", 0)
        }

        # Conversion en CSV dans un buffer mémoire
        # On crée d'abord l'entête CSV et la ligne unique de valeurs
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

        # Retourne les données météo sous forme de JSON
        return {
            "message": f"Données météo pour lat={lat}, lon={lon} stockées sous '{filename}'.",
            "weather_data": weather_info
        }

    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Erreur de connexion à l'API OpenWeather: {str(e)}")

@app.get("/healthcheck")
def health_check():
    """
    Vérifie si l'API est en cours d'exécution.
    """
    return {"status": "API en cours d'exécution."}


