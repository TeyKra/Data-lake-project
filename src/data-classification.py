import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import seaborn as sns

# =================================================================
#                    S3 FUNCTIONS
# =================================================================

def download_file_from_s3(bucket_name, object_name, s3_client):
    """
    Downloads a CSV file from S3 and loads it into a Pandas DataFrame.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_name)
        csv_content = response["Body"].read()
        print(f"[SUCCESS] File '{object_name}' downloaded from bucket '{bucket_name}'.")
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

def upload_file_to_s3(buffer, bucket_name, object_name, s3_client):
    """
    Uploads a file (e.g., an image from a memory buffer) to an S3 bucket.
    """
    try:
        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=buffer.getvalue())
        print(f"[SUCCESS] File '{object_name}' uploaded successfully to bucket '{bucket_name}'.")
    except Exception as e:
        print(f"[ERROR] Error uploading file '{object_name}' to S3: {e}")

# =================================================================
#                    FUNCTION TO ASSIGN NAMES TO CLUSTERS
# =================================================================

def get_cluster_names(cluster_labels):
    """
    Maps each cluster label to a descriptive name based on observed patterns.
    """
    cluster_name_mapping = {
        0: "Cool & Dry",         # Example: Low temperatures, low humidity
        1: "Warm & Humid",       # Example: Moderate temperatures, high humidity
        2: "Hot & Arid",         # Example: High temperatures, low humidity
        3: "Extreme Weather"     # Example: Extreme conditions, may vary
    }
    return [cluster_name_mapping.get(label, "Unknown Cluster") for label in cluster_labels]

# =================================================================
#                    MAIN K-MEANS CLUSTERING FUNCTION
# =================================================================

def perform_kmeans_clustering(df, features, n_clusters=4, random_state=42):
    """
    Performs K-Means clustering on the 'features' columns of the DataFrame df.
    - features: list of numeric columns to use for clustering.
    - n_clusters: number of K-Means clusters.
    - random_state: random seed for reproducibility.
    
    Returns the DataFrame with two new columns:
    - 'cluster_label': cluster number
    - 'cluster_name': descriptive name of the cluster
    """

    # Check that all 'features' columns exist in the DataFrame
    missing_features = [col for col in features if col not in df.columns]
    if missing_features:
        print(f"[WARNING] The following columns do not exist in the DataFrame and will be ignored: {missing_features}")
        features = [col for col in features if col in df.columns]

    if not features:
        print("[ERROR] No valid numeric column for clustering.")
        return df

    # Retrieve only the necessary numeric columns
    df_subset = df[features].copy()

    # Standardize the data (important for K-Means)
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(df_subset)

    # Create and train the K-Means model
    kmeans = KMeans(n_clusters=n_clusters, random_state=random_state)
    kmeans.fit(scaled_data)

    # Retrieve the labels
    cluster_labels = kmeans.labels_

    # Add the 'cluster_label' column to the original DataFrame
    df["cluster_label"] = cluster_labels

    # Add descriptive cluster names
    df["cluster_name"] = get_cluster_names(cluster_labels)

    # Display some clustering information
    print("[INFO] K-Means Inertia:", kmeans.inertia_)
    print("[INFO] K-Means Centroids:\n", kmeans.cluster_centers_)
    print(f"[INFO] K-Means clustering with {n_clusters} clusters completed.")
    return df

# =================================================================
#                    FUNCTION TO VISUALIZE CLUSTERS
# =================================================================

def visualize_clusters_to_s3(df, features, bucket_name, object_name, s3_client):
    """
    Visualizes the clusters with dimensionality reduction via PCA.
    Saves the plot in memory (buffer) and uploads it directly to S3.
    """
    # Standardize the data for PCA
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(df[features].fillna(df[features].mean()))

    # Dimensionality reduction with PCA
    pca = PCA(n_components=2)
    reduced_data = pca.fit_transform(scaled_data)

    # Add the reduced dimensions to the DataFrame
    df["PCA1"] = reduced_data[:, 0]
    df["PCA2"] = reduced_data[:, 1]

    # Visualization with Seaborn
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
    plt.title("City Clusters Based on Weather Patterns")
    plt.xlabel("Principal Component 1 (PCA1)")
    plt.ylabel("Principal Component 2 (PCA2)")
    plt.legend(title="Clusters", loc="upper right")
    plt.grid(True)

    # Save the plot in memory (buffer)
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    buffer.seek(0)  # Rewind the buffer to the beginning
    plt.close()  # Close the plot to free memory

    # Upload the plot from the buffer to S3
    upload_file_to_s3(buffer, bucket_name, object_name, s3_client)

# =================================================================
#                    MAIN FUNCTION
# =================================================================

def main():
    # Connection parameters for LocalStack / S3
    endpoint_url = "http://localstack-data-lake-project:4566"
    aws_access_key_id = "test"
    aws_secret_access_key = "test"

    # S3 Buckets and objects
    source_bucket = "staging"  
    source_object = "global_weather_data.csv"  # Name of the preprocessed CSV
    output_bucket = "curated"                  # You can choose "analytics" or another bucket
    output_object = "weather_clusters.csv"     # Name of the output CSV with clusters
    output_graph_object = "weather_clusters.png"  # Name of the PNG image of the plot

    # Initialize the S3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    # Retrieve the DataFrame from S3
    df_weather = download_file_from_s3(source_bucket, source_object, s3_client)
    if df_weather is None or df_weather.empty:
        print("[ERROR] No data to process or the DataFrame is empty.")
        return

    # Select the relevant columns for clustering
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

    # Run K-Means on these features
    df_with_clusters = perform_kmeans_clustering(
        df_weather,
        features=features_for_clustering,
        n_clusters=4,          # Desired number of clusters
        random_state=42
    )

    # Upload the new DataFrame with the 'cluster_label' and 'cluster_name' columns
    upload_dataframe_to_s3(df_with_clusters, output_bucket, output_object, s3_client)

    # Generate and upload the clusters plot
    visualize_clusters_to_s3(df_with_clusters, features_for_clustering, output_bucket, output_graph_object, s3_client)

# Script entry point
if __name__ == "__main__":
    main()
