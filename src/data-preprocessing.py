import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from numba import njit, prange

# =================================================================
#                    FONCTIONS NUMBA COMPILÉES
# =================================================================

@njit
def get_season_num(day_of_year, lat):
    """
    Renvoie un entier correspondant à la saison.
    0 -> Spring / 1 -> Summer / 2 -> Autumn / 3 -> Winter
    """
    if lat > 0:
        # Hémisphère Nord
        if 80 <= day_of_year < 172:
            return 0  # Spring
        elif 172 <= day_of_year < 264:
            return 1  # Summer
        elif 264 <= day_of_year < 355:
            return 2  # Autumn
        else:
            return 3  # Winter
    else:
        # Hémisphère Sud
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
    Renvoie un entier correspondant à la catégorie de température.
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
    Calcule pour chaque ligne la saison et la catégorie de température
    en se basant sur le day_of_year (jour de l'année), la latitude et
    la température. Retourne deux tableaux d'entiers.
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
#                    FONCTIONS S3 ET DE TRAITEMENT
# =================================================================

def list_all_files_in_s3(bucket_name, prefix, s3_client):
    """
    Récupère la liste de tous les fichiers présents dans un bucket S3 
    correspondant au préfixe spécifié.
    """
    files = []
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" not in response:
            print(f"[INFO] Aucun fichier trouvé avec le préfixe '{prefix}' dans le bucket '{bucket_name}'.")
            return files

        for obj in response["Contents"]:
            files.append(obj["Key"])

        print(f"[SUCCESS] Fichiers trouvés avec le préfixe '{prefix}': {files}")
    except Exception as e:
        print(f"[ERROR] Erreur lors de la récupération des fichiers depuis S3 : {e}")
    return files

def download_file_from_s3(bucket_name, object_name, s3_client):
    """
    Télécharge un fichier CSV depuis S3 et le charge dans un DataFrame Pandas.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
        csv_content = response["Body"].read()
        print(f"[SUCCESS] Fichier '{object_name}' téléchargé depuis S3.")
        return pd.read_csv(BytesIO(csv_content))
    except Exception as e:
        print(f"[ERROR] Erreur lors du téléchargement du fichier '{object_name}' depuis S3 : {e}")
        return None

def upload_dataframe_to_s3(df, bucket_name, object_name, s3_client):
    """
    Convertit un DataFrame en fichier CSV et l'envoie directement vers S3.
    """
    try:
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())
        print(f"[SUCCESS] Fichier '{object_name}' téléchargé avec succès dans le bucket '{bucket_name}'.")
    except Exception as e:
        print(f"[ERROR] Erreur lors du téléchargement du fichier '{object_name}' vers S3 : {e}")

# =================================================================
#                FONCTION PRINCIPALE DE PRÉTRAITEMENT
# =================================================================

def preprocess_data(df):
    print("[INFO] Début du prétraitement des données...")

    # -------------------------------------------------------------------------
    # 1. SUPPRESSION DES DOUBLONS
    # -------------------------------------------------------------------------
    duplicates = df[df.duplicated()]
    if not duplicates.empty:
        print(f"[INFO] Doublons trouvés et supprimés : {len(duplicates)}")
    df = df.drop_duplicates()
    print("[SUCCESS] Suppression des doublons réalisée.")

    # -------------------------------------------------------------------------
    # 2. SUPPRIMER LES LIGNES AVEC 'cod' != 200
    # -------------------------------------------------------------------------
    if "cod" in df.columns:
        rows_to_delete = df[df["cod"] != 200]
        if not rows_to_delete.empty:
            print(f"[INFO] Lignes avec 'cod' != 200 supprimées : {len(rows_to_delete)}")
        df = df[df["cod"] == 200]
        print("[SUCCESS] Suppression des lignes avec 'cod' != 200 réalisée.")
    else:
        print("[INFO] Colonne 'cod' non présente, aucune suppression basée sur 'cod'.")

    # -------------------------------------------------------------------------
    # 3. AJOUT DE LA COLONNE 'local_datetime'
    # -------------------------------------------------------------------------
    # On vérifie la présence des colonnes "dt" et "timezone"
    if "dt" in df.columns and "timezone" in df.columns:
        df["local_datetime"] = pd.to_datetime(df["dt"], unit="s") + pd.to_timedelta(df["timezone"], unit="s")
        print("[SUCCESS] Colonne 'local_datetime' ajoutée.")
    else:
        print("[INFO] Impossible de calculer 'local_datetime' : colonnes 'dt' ou 'timezone' manquantes.")

    # -------------------------------------------------------------------------
    # 4. SUPPRESSION DES COLONNES INUTILES
    # -------------------------------------------------------------------------
    columns_to_drop = ["dt", "timezone", "id", "base", "cod"]
    existing_to_drop = [col for col in columns_to_drop if col in df.columns]
    df.drop(columns=existing_to_drop, inplace=True, errors='ignore')
    print(f"[SUCCESS] Colonnes supprimées (si présentes) : {existing_to_drop}")

    # -------------------------------------------------------------------------
    # 5. CONVERSION DES TIMESTAMPS UNIX EN FORMAT DATETIME
    # -------------------------------------------------------------------------
    for col in ["sunrise", "sunset"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit="s")
    print("[SUCCESS] Conversion des timestamps UNIX réalisée pour 'sunrise' et 'sunset' (si présentes).")

    # -------------------------------------------------------------------------
    # 6. CONVERSION DES TEMPÉRATURES DE KELVIN À CELSIUS
    # -------------------------------------------------------------------------
    temperature_columns = ["temp", "feels_like", "temp_min", "temp_max"]
    for col in temperature_columns:
        if col in df.columns:
            df[col] = df[col] - 273.15
    print("[SUCCESS] Conversion des températures de Kelvin à Celsius réalisée (si colonnes présentes).")

    # -------------------------------------------------------------------------
    # 7. AJOUT DE LA DURÉE DU JOUR (DAYLIGHT_DURATION)
    # -------------------------------------------------------------------------
    if "sunrise" in df.columns and "sunset" in df.columns:
        df["daylight_duration"] = (df["sunset"] - df["sunrise"]).dt.total_seconds() / 3600
        print("[SUCCESS] Colonne 'daylight_duration' ajoutée.")
    else:
        print("[INFO] Impossible de calculer 'daylight_duration' : colonnes 'sunrise' ou 'sunset' manquantes.")

    # -------------------------------------------------------------------------
    # 8. CALCUL DE LA DIFFÉRENCE DE TEMPÉRATURE
    # -------------------------------------------------------------------------
    if "temp_min" in df.columns and "temp_max" in df.columns:
        df["temperature_difference"] = df["temp_max"] - df["temp_min"]
        print("[SUCCESS] Colonne 'temperature_difference' ajoutée.")
    else:
        print("[INFO] Impossible de calculer 'temperature_difference' : colonnes 'temp_min' ou 'temp_max' manquantes.")
    # -------------------------------------------------------------------------
    # 9. CALCUL DE L'INDICE DE CONFORT THERMIQUE (THERMAL_COMFORT_INDEX)
    # -------------------------------------------------------------------------
    if "temp" in df.columns and "humidity" in df.columns and "speed" in df.columns:
        df["thermal_comfort_index"] = (
            df["temp"] - (0.55 * (1 - (df["humidity"] / 100)) * (df["temp"] - 14.5)) - (0.2 * df["speed"])
        )
        print("[SUCCESS] Colonne 'thermal_comfort_index' ajoutée.")
    else:
        print("[INFO] Impossible de calculer 'thermal_comfort_index' : colonnes 'temp', 'humidity' ou 'speed' manquantes.")

    # -------------------------------------------------------------------------
    # 10. CALCUL MASSIF DE 'season' ET 'temperature_category' VIA NUMBA
    # -------------------------------------------------------------------------
    if "local_datetime" in df.columns and "lat" in df.columns and "temp" in df.columns:
        df["day_of_year"] = df["local_datetime"].dt.dayofyear

        # Récupération des arrays nécessaires
        day_of_year_arr = df["day_of_year"].values
        lat_arr         = df["lat"].values
        temp_arr        = df["temp"].values

        # Calcul via Numba
        season_codes, temp_cat_codes = compute_season_and_temp_category(day_of_year_arr, lat_arr, temp_arr)

        # Mappage des codes vers chaînes de caractères
        df["season"] = [map_season_code_to_str(code) for code in season_codes]
        df["temperature_category"] = [map_temp_cat_code_to_str(code) for code in temp_cat_codes]

        # On peut supprimer la colonne day_of_year si inutile
        df.drop(columns=["day_of_year"], inplace=True)
        print("[SUCCESS] Colonnes 'season' et 'temperature_category' ajoutées (via Numba).")
    else:
        print("[INFO] Impossible de calculer 'season' et 'temperature_category' : colonnes 'local_datetime', 'lat' ou 'temp' manquantes.")

    # -------------------------------------------------------------------------
    # 11. RENOMMAGE DES COLONNES
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
    print("[SUCCESS] Renommage des colonnes réalisé.")

    # -------------------------------------------------------------------------
    # 12. RÉORGANISATION DES COLONNES
    # -------------------------------------------------------------------------
    column_order = [
        "country", "city", "longitude", "latitude", "weather_Condition", "weather_description",
        "temperature", "temperature_category", "feels_like_temperature", "min_temperature", "max_temperature",
        "temperature_difference", "thermal_comfort_index", "pressure", "sea_level_pressure",
        "ground_level_pressure", "humidity", "visibility", "wind_Speed", "wind_direction", "cloud_cover",
        "sunrise_time", "sunset_time", "daylight_duration", "local_datetime", "season"
    ]
    # Vérifier que toutes les colonnes existent (au cas où certaines seraient manquantes dans les données)
    column_order_existing = [col for col in column_order if col in df.columns]
    df = df[column_order_existing]
    print("[SUCCESS] Colonnes réorganisées.")

    print("[SUCCESS] Prétraitement des données terminé.")
    return df

# =================================================================
#                       FONCTION MAIN
# =================================================================

def main():
    bucket_name = "raw"
    staging_bucket = "staging"
    endpoint_url = "http://localhost:4566"

    # Préfixes à récupérer
    prefixes_to_fetch = ["weather_data_", "user_input_data_"]

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="test",
        aws_secret_access_key="test"
    )

    # Récupérer la liste de tous les fichiers pour chaque préfixe
    all_files = []
    for prefix in prefixes_to_fetch:
        files_list = list_all_files_in_s3(bucket_name, prefix, s3_client)
        all_files.extend(files_list)  # On ajoute à la liste globale

    # S'assurer qu'on a bien récupéré au moins un fichier
    if not all_files:
        print("[INFO] Aucun fichier trouvé avec les préfixes demandés. Fin du programme.")
        return

    # Télécharger tous les fichiers et les fusionner en un seul DataFrame
    list_df = []
    for file_key in all_files:
        df_weather = download_file_from_s3(bucket_name, file_key, s3_client)
        if df_weather is not None:
            list_df.append(df_weather)

    if not list_df:
        print("[INFO] Impossible de créer un DataFrame à partir des fichiers téléchargés. Fin du programme.")
        return

    # Concaténer tous les DataFrames en un seul
    merged_df = pd.concat(list_df, ignore_index=True)

    # Prétraiter les données
    merged_df = preprocess_data(merged_df)

    # Nom du fichier de sortie
    output_file_name = "global_weather_data.csv"

    # Uploader le DataFrame final sous un nom fixe dans le bucket staging
    upload_dataframe_to_s3(merged_df, staging_bucket, output_file_name, s3_client)


if __name__ == "__main__":
    main()
