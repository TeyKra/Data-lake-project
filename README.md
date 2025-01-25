# Data Lake Project

This project demonstrates a simple setup for managing buckets and files using LocalStack and a custom API.

## Docker Commands

### Build and Start Containers
- Build images without using the cache:
  ```bash
  docker compose build --no-cache
  ```

- Start containers in detached mode:
  ```bash
  docker compose up -d
  ```

### Check Logs
- Check logs for all containers:
  ```bash
  docker-compose logs -f
  ```

- Check logs for the Airflow Scheduler:
  ```bash
  docker logs -f airflow-scheduler-data-lake-project
  ```

- Check logs for the Airflow Webserver:
  ```bash
  docker logs -f airflow-webserver-data-lake-project
  ```

- Check logs for Localstack:
  ```bash
  docker logs -f localstack-data-lake-project
  ```

## LocalStack Setup

### Creating Buckets
Use the following commands to create the required buckets in LocalStack:

- **Create the `raw` bucket:**
  ```bash
  aws s3api create-bucket --bucket raw --endpoint-url=http://localhost:4566
  ```

- **Create the `staging` bucket:**
  ```bash
  aws s3api create-bucket --bucket staging --endpoint-url=http://localhost:4566
  ```

- **Create the `curated` bucket:**
  ```bash
  aws s3api create-bucket --bucket curated --endpoint-url=http://localhost:4566
  ```
aws --endpoint-url=http://localhost:4566 s3api list-buckets

aws --endpoint-url=http://localhost:4566 s3 ls s3://raw

aws --endpoint-url=http://localhost:4566 s3 rm s3://raw --recursive

---

## Starting the API

Run the following command to start the API server:
```bash
uvicorn src.api:app --reload
```

---

## API Endpoints

### 1. **List Available Buckets**
Retrieve the list of all buckets:
```bash
curl -X GET http://127.0.0.1:8000/buckets
```

### 2. **List Files in a Bucket**
Get the list of files in a specific bucket:
```bash
curl -X GET http://127.0.0.1:8000/files/<bucket_name>
```

### 3. **Download a File**
Download a file from a specific bucket:
```bash
curl -X GET http://127.0.0.1:8000/download/<bucket_name>/<file_name>
```

### 4. **Upload a File**
Upload a file to a specific bucket:
```bash
curl -X POST -F "file=@<file_path>" http://127.0.0.1:8000/upload/<bucket_name>
```

### 5. **Delete a File**
Delete a file from a specific bucket:
```bash
curl -X DELETE http://127.0.0.1:8000/delete/<bucket_name>/<file_name>
```

### 6. **API Health Check**
Verify the API's health status:
```bash
curl -X GET http://127.0.0.1:8000/healthcheck
```

---

# Step Process to Use the OpenWeather Data Lake Pipeline Application

## Check the Creation and Content of Each Data Lake Layer

### 1. Access the LocalStack Container
Run the following command to access the container:
```bash
docker exec -it localstack-data-lake-project bash
```

### 2. Configure AWS CLI
Set up the AWS CLI within the container:
```bash
aws configure

AWS Access Key ID [None]: test
AWS Secret Access Key [None]: test
Default region name [None]: us-east-1
Default output format [None]: json
```

### 3. List All S3 Buckets
Run the following command to list all S3 buckets:
```bash
aws --endpoint-url=http://localhost:4566 s3 ls
```

#### Output:
```
2025-01-25 13:17:59 raw
2025-01-25 13:17:59 staging
2025-01-25 13:17:59 curated
```

### 4. Check the Content of Each Bucket

#### Raw Bucket
List the content of the `raw` bucket:
```bash
aws --endpoint-url=http://localhost:4566 s3 ls s3://raw
```

#### Output:
```
2025-01-25 13:18:01      43038 weather_data_2025-01-25_13-18-01.csv
```

#### Staging Bucket
List the content of the `staging` bucket:
```bash
aws --endpoint-url=http://localhost:4566 s3 ls s3://staging
```

#### Output:
```
2025-01-25 13:20:16     118435 global_weather_data.csv
```

#### Curated Bucket
List the content of the `curated` bucket:
```bash
aws --endpoint-url=http://localhost:4566 s3 ls s3://curated
```

#### Output:
```
2025-01-25 13:20:19     123299 weather_clusters.csv
2025-01-25 13:20:19     122336 weather_clusters.png
```
