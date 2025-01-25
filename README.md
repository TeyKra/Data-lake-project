# Data Lake Project

This project demonstrates a simple setup for managing buckets and files using LocalStack and a custom API.

---

## Docker 
docker compose build --no-cache

docker compose up -d

check all container logs
docker-compose logs -f 

check airflow-scheduler logs
docker logs -f airflow-scheduler-data-lake-project

check airflow-webserver logs
docker logs -f airflow-webserver-data-lake-project

check localstack logs
docker logs -f localstack-data-lake-project

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


Step process to use the openweather data lake pipeline application : 



check the creation and content of each data lake layers :
docker exec -it localstack-data-lake-project bash

aws configure

AWS Access Key ID [None]: test
AWS Secret Access Key [None]: test
Default region name [None]: us-east-1
Default output format [None]: json

list all S3 bucket
aws --endpoint-url=http://localhost:4566 s3 ls

2025-01-25 13:17:59 raw
2025-01-25 13:17:59 staging
2025-01-25 13:17:59 curated

list the content of each bucket: 
root@42ac4b19d57a:/opt/code/localstack# aws --endpoint-url=http://localhost:4566 s3 ls s3://raw
2025-01-25 13:18:01      43038 weather_data_2025-01-25_13-18-01.csv

root@42ac4b19d57a:/opt/code/localstack# aws --endpoint-url=http://localhost:4566 s3 ls s3://staging
2025-01-25 13:20:16     118435 global_weather_data.csv

root@42ac4b19d57a:/opt/code/localstack# aws --endpoint-url=http://localhost:4566 s3 ls s3://curated
2025-01-25 13:20:19     123299 weather_clusters.csv
2025-01-25 13:20:19     122336 weather_clusters.png

