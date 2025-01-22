# Data Lake Project

This project demonstrates a simple setup for managing buckets and files using LocalStack and a custom API.

---

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

## Notes

- Replace `<bucket_name>` with the name of your bucket.
- Replace `<file_name>` with the name of your file.
- Replace `<file_path>` with the full path to the file you want to upload.

