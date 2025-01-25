import aiohttp
import asyncio
import json
import polars as pl
import os
import boto3
from io import BytesIO
from datetime import datetime

# Clé API OpenWeather
API_KEY = "b022acb509eacae0875ded1afe41a527"

# URL de l'API OpenWeather
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# URL de l'API REST Countries
COUNTRIES_API_URL = "https://restcountries.com/v3.1/all?fields=name,capital"

# Dictionnaire des correspondances pour normaliser les noms des capitales
CAPITALS_MAPPING = {
    "Papeetē": "Papeete",
    "St. Peter Port": "Saint-Pierre-Port"
}

# =================================================================
#           FONCTION DE RÉCUPÉRATION DES CAPITALES VIA API
# =================================================================

def normalize_capital_name(capital):
    """
    Normalise le nom des capitales en utilisant le dictionnaire de correspondance.
    """
    return CAPITALS_MAPPING.get(capital, capital)

async def fetch_capitals_from_api():
    """
    Récupère les noms des capitales et des pays depuis l'API REST Countries.
    Exclut les entrées dont la capitale est 'No Capital'.
    Normalise les noms des capitales mal formatés.
    """
    print("[INFO] Récupération des capitales via l'API REST Countries...")
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

                        # Normaliser les noms des capitales
                        capital = normalize_capital_name(capital)

                        # Exclure les capitales avec la valeur 'No Capital'
                        if capital != 'No Capital':
                            capitals.append({"country": name, "city": capital})

                    print(f"[SUCCESS] Récupération de {len(capitals)} capitales (exclusion des 'No Capital').")
                    return capitals
                else:
                    print(f"[ERROR] Erreur {response.status} lors de l'appel à l'API REST Countries.")
                    return []
        except Exception as e:
            print(f"[ERROR] Erreur lors de la récupération des capitales : {e}")
            return []

# =================================================================
#        FONCTIONS ASYNCHRONES POUR RÉCUPÉRER LES DONNÉES MÉTÉO
# =================================================================
async def fetch_weather_data(session, api_key, city):
    """
    Appelle l'API OpenWeather de manière asynchrone pour récupérer
    les données météo d'une ville.
    """
    params = {"appid": api_key, "lang": "fr", "q": city}
    try:
        async with session.get(BASE_URL, params=params) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"[ERROR] Erreur {response.status} pour la ville '{city}'.")
                return {"error": f"Status {response.status}"}
    except Exception as e:
        print(f"[ERROR] Erreur lors de l'appel à l'API pour '{city}' : {e}")
        return {"error": str(e)}

def fetch_weather_for_all_capitals(api_key, capitals):
    """
    Wrapper pour exécuter l'appel asynchrone en mode synchrone.
    """
    return asyncio.run(fetch_weather_for_all_capitals_async(api_key, capitals))

async def fetch_weather_for_all_capitals_async(api_key, capitals):
    """
    Récupère les données météo pour une liste de capitales
    de manière asynchrone.
    """
    print("[INFO] Démarrage de la récupération des données météo pour toutes les capitales...")
    error_log = []
    weather_data = {}

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_weather_data(session, api_key, capital["city"]) for capital in capitals]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for capital, result in zip(capitals, results):
            city = capital["city"]
            country = capital["country"]
            if isinstance(result, dict) and "error" not in result:
                print(f"[SUCCESS] Données météo récupérées pour la ville '{city}', pays '{country}'.")
                weather_data[city] = result
            else:
                print(f"[ERROR] Données ignorées pour la ville '{city}', pays '{country}' : {result}")
                error_log.append({"city": city, "country": country, "error": result})
                weather_data[city] = {"error": "Erreur lors de la récupération"}

    # Création d'un fichier log pour les erreurs
    log_dir = "src/logs"
    os.makedirs(log_dir, exist_ok=True)
    current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file_path = os.path.join(log_dir, f"errors_data_recovery_{current_date}.log")

    with open(log_file_path, "w", encoding="utf-8") as error_file:
        json.dump(error_log, error_file, indent=4)

    print(f"[INFO] Les éventuelles erreurs ont été enregistrées dans '{log_file_path}'.")
    return weather_data

# =================================================================
#     FONCTION DE CONVERSION DES DONNÉES VERS DATAFRAME POLARS
# =================================================================
def convert_weather_data_to_dataframe(weather_data, capitals):
    """
    Structure les données météo en un DataFrame Polars.
    """
    print("[INFO] Conversion des données météo en DataFrame Polars...")
    structured_data = []

    for capital in capitals:
        city = capital["city"]
        country = capital["country"]
        data = weather_data.get(city, {})

        # Ignorer les entrées contenant une erreur
        if "error" in data:
            print(f"[ERROR] Données de la ville '{city}', pays '{country}' ignorées (erreur présente).")
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
    print("[SUCCESS] DataFrame Polars créé avec succès.")
    return df

# =================================================================
#       FONCTION POUR UPLOADER LE DATAFRAME DANS S3 LOCALSTACK
# =================================================================
def upload_dataframe_to_s3(df, bucket_name, object_name, endpoint_url="http://localstack-data-lake-project:4566"):
    """
    Upload un DataFrame Polars directement dans un bucket S3 LocalStack
    sans sauvegarde locale.
    """
    print("[INFO] Début de l'upload du DataFrame vers S3...")
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
        print(f"[SUCCESS] Fichier '{object_name}' uploadé avec succès dans le bucket '{bucket_name}'.")
    except Exception as e:
        print(f"[ERROR] Erreur lors de l'upload du fichier '{object_name}' vers S3 : {e}")

# =================================================================
#                   FONCTION PRINCIPALE
# =================================================================
def main():
    print("[INFO] Début du script data-recovery.")

    # Récupération des capitales via l'API REST Countries
    capitals = asyncio.run(fetch_capitals_from_api())
    if not capitals:
        print("[ERROR] Aucune capitale n'a été récupérée. Fin du programme.")
        return

    # Récupérer les données météo pour toutes les capitales
    all_weather_data = fetch_weather_for_all_capitals(API_KEY, capitals)

    # Convertir les données collectées en DataFrame Polars
    df_weather = convert_weather_data_to_dataframe(all_weather_data, capitals)

    # Générer le nom du fichier avec la date/heure actuelle
    current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    object_name = f"weather_data_{current_date}.csv"
    bucket_name = "raw"

    # Uploader le DataFrame directement dans le bucket S3 LocalStack
    upload_dataframe_to_s3(df_weather, bucket_name, object_name)

    print("[SUCCESS] Fin du script data-recovery.")

# =================================================================
#                          ENTRY POINT
# =================================================================
if __name__ == "__main__":
    main()
