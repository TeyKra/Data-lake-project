import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import seaborn as sns
# -------------------------------------------------------------------------
#                     FONCTIONS UTILES POUR S3
# -------------------------------------------------------------------------

def download_file_from_s3(bucket_name, object_name, s3_client):
    """
    Télécharge un fichier CSV depuis S3 et le charge dans un DataFrame Pandas.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
        csv_content = response["Body"].read()
        print(f"[SUCCESS] Fichier '{object_name}' téléchargé depuis le bucket '{bucket_name}'.")
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
        print(f"[SUCCESS] Fichier '{object_name}' uploadé avec succès dans le bucket '{bucket_name}'.")
    except Exception as e:
        print(f"[ERROR] Erreur lors de l'upload du fichier '{object_name}' vers S3 : {e}")

def upload_file_to_s3(buffer, bucket_name, object_name, s3_client):
    """
    Upload un fichier (ex : image depuis un buffer mémoire) vers un bucket S3.
    """
    try:
        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=buffer.getvalue())
        print(f"[SUCCESS] Fichier '{object_name}' uploadé avec succès dans le bucket '{bucket_name}'.")
    except Exception as e:
        print(f"[ERROR] Erreur lors de l'upload du fichier '{object_name}' vers S3 : {e}")


# -------------------------------------------------------------------------
#       FONCTION POUR ATTRIBUER DES NOMS AUX CLUSTERS
# -------------------------------------------------------------------------

def get_cluster_names(cluster_labels):
    """
    Mappe chaque cluster_label à un nom descriptif basé sur les patterns observés.
    """
    cluster_name_mapping = {
        0: "Cool & Dry",         # Exemple: Températures basses, faible humidité
        1: "Warm & Humid",       # Exemple: Températures modérées, forte humidité
        2: "Hot & Arid",         # Exemple: Températures élevées, faible humidité
        3: "Extreme Weather"     # Exemple: Conditions extrêmes, peut varier
    }
    return [cluster_name_mapping.get(label, "Unknown Cluster") for label in cluster_labels]


# -------------------------------------------------------------------------
#             FONCTION PRINCIPALE DE CLUSTERING (K-MEANS)
# -------------------------------------------------------------------------
def perform_kmeans_clustering(df, features, n_clusters=4, random_state=42):
    """
    Effectue un clustering K-Means sur les colonnes 'features' du DataFrame df.
    - features: liste de colonnes (numériques) à utiliser pour le clustering.
    - n_clusters: nombre de clusters K-Means.
    - random_state: graine aléatoire pour la reproductibilité.
    
    Retourne le DataFrame avec deux nouvelles colonnes :
    - 'cluster_label' : numéro du cluster
    - 'cluster_name' : nom descriptif du cluster
    """

    # Vérifier que toutes les colonnes 'features' existent dans le df
    missing_features = [col for col in features if col not in df.columns]
    if missing_features:
        print(f"[WARNING] Les colonnes suivantes n'existent pas dans le DataFrame et seront ignorées: {missing_features}")
        features = [col for col in features if col in df.columns]

    if not features:
        print("[ERROR] Aucune colonne numérique valide pour le clustering.")
        return df

    # On récupère uniquement les colonnes numériques nécessaires
    df_subset = df[features].copy()

    # Standardisation des données (important pour K-Means)
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(df_subset)

    # Création et entraînement du modèle K-Means
    kmeans = KMeans(n_clusters=n_clusters, random_state=random_state)
    kmeans.fit(scaled_data)

    # Récupération des labels
    cluster_labels = kmeans.labels_

    # Ajout de la colonne 'cluster_label' au DataFrame original
    df["cluster_label"] = cluster_labels

    # Ajout des noms descriptifs des clusters
    df["cluster_name"] = get_cluster_names(cluster_labels)

    # Affiche quelques informations sur le clustering
    print("[INFO] Inertie (K-Means inertia) :", kmeans.inertia_)
    print("[INFO] Centroides du modèle K-Means :\n", kmeans.cluster_centers_)
    print(f"[INFO] Clustering K-Means avec {n_clusters} clusters terminé.")
    return df

# -------------------------------------------------------------------------
#             FONCTION POUR VISUALISER LES CLUSTERS
# -------------------------------------------------------------------------

def visualize_clusters_to_s3(df, features, bucket_name, object_name, s3_client):
    """
    Visualise les clusters avec une réduction de dimension via PCA.
    Sauvegarde le graphique en mémoire (buffer) et l'upload directement dans S3.
    """
    # Standardisation des données pour PCA
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(df[features].fillna(df[features].mean()))

    # Réduction de dimension avec PCA
    pca = PCA(n_components=2)
    reduced_data = pca.fit_transform(scaled_data)

    # Ajout des dimensions réduites au DataFrame
    df["PCA1"] = reduced_data[:, 0]
    df["PCA2"] = reduced_data[:, 1]

    # Visualisation avec Seaborn
    plt.figure(figsize=(10, 8))
    sns.scatterplot(
        x="PCA1",
        y="PCA2",
        hue="cluster_name",
        palette="Set1",
        data=df,
        s=100,
        alpha=0.8
    )
    plt.title("Clusters des villes selon leurs patterns météo")
    plt.xlabel("Composante principale 1 (PCA1)")
    plt.ylabel("Composante principale 2 (PCA2)")
    plt.legend(title="Clusters", loc="upper right")
    plt.grid(True)

    # Sauvegarder le graphique en mémoire (buffer)
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    buffer.seek(0)  # Revenir au début du buffer
    plt.close()  # Fermer le graphique pour libérer la mémoire

    # Upload le graphique depuis le buffer vers S3
    upload_file_to_s3(buffer, bucket_name, object_name, s3_client)

# -------------------------------------------------------------------------
#                           FONCTION MAIN
# -------------------------------------------------------------------------
def main():
    # Paramètres de connexion à LocalStack / S3
    endpoint_url = "http://localstack-data-lake-project:4566"
    aws_access_key_id = "test"
    aws_secret_access_key = "test"

    # Buckets et objets S3
    source_bucket = "staging"  
    source_object = "global_weather_data.csv"  # Nom du CSV prétraité
    output_bucket = "curated"                  # Tu peux choisir "analytics" ou autre
    output_object = "weather_clusters.csv"     # Nom du CSV de sortie avec clusters
    output_graph_object = "weather_clusters.png"  # Nom de l'image PNG du graphique

    # Initialisation du client S3
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    # 1) Récupérer le DataFrame depuis S3
    df_weather = download_file_from_s3(source_bucket, source_object, s3_client)
    if df_weather is None or df_weather.empty:
        print("[ERROR] Aucune donnée à traiter ou DataFrame vide.")
        return

    # 2) Sélection des colonnes pertinentes pour le clustering
    features_for_clustering = [
        "temperature",
        "feels_like_temperature",
        "temperature_difference",
        "thermal_comfort_index",
        "humidity",
        "wind_Speed",
        "pressure",
        "cloud_cover"
    ]

    # 3) Lancement d'un K-Means sur ces features
    df_with_clusters = perform_kmeans_clustering(
        df_weather,
        features=features_for_clustering,
        n_clusters=4,          # Nombre de clusters souhaités
        random_state=42
    )

    # 4) Upload du nouveau DataFrame avec les colonnes 'cluster_label' et 'cluster_name'
    upload_dataframe_to_s3(df_with_clusters, output_bucket, output_object, s3_client)

    # 5) Génération et upload du graphique des clusters
    visualize_clusters_to_s3(df_with_clusters, features_for_clustering, output_bucket, output_graph_object, s3_client)

# Point d'entrée du script
if __name__ == "__main__":
    main()
