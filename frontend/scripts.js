// =================================================================
//            BASE URL CONFIGURATION FOR THE API
// =================================================================
const API_BASE_URL = "http://localhost:8000";

// =================================================================
//            FUNCTION TO LIST BUCKETS
// =================================================================
document.getElementById("list-buckets").addEventListener("click", async () => {
    try {
        // Retrieve the list of buckets from the API endpoint
        const response = await fetch(`${API_BASE_URL}/buckets`);
        if (!response.ok) {
            throw new Error("Error retrieving buckets.");
        }
        const data = await response.json();

        // Clear the existing list and populate it with retrieved buckets
        const bucketsList = document.getElementById("buckets-list");
        bucketsList.innerHTML = "";
        data.buckets.forEach(bucket => {
            const li = document.createElement("li");
            li.textContent = bucket;
            bucketsList.appendChild(li);
        });
    } catch (error) {
        alert("Error retrieving buckets: " + error);
    }
});

// =================================================================
//            FUNCTION TO LIST FILES IN A SPECIFIC BUCKET
// =================================================================
document.getElementById("list-files").addEventListener("click", async () => {
    const bucketName = document.getElementById("bucket-name").value;
    if (!bucketName) {
        alert("Please enter a bucket.");
        return;
    }
    try {
        // Fetch the list of files from the specified bucket
        const response = await fetch(`${API_BASE_URL}/files/${bucketName}`);
        if (!response.ok) {
            throw new Error("Error retrieving files.");
        }
        const data = await response.json();

        // Clear the file list and display each file as a list item
        const filesList = document.getElementById("files-list");
        filesList.innerHTML = "";
        data.files.forEach(file => {
            const li = document.createElement("li");
            li.textContent = file;
            filesList.appendChild(li);
        });
    } catch (error) {
        alert("Error retrieving files: " + error);
    }
});

// =================================================================
//            FUNCTION TO DOWNLOAD A FILE FROM A BUCKET
// =================================================================
document.getElementById("download-button").addEventListener("click", async () => {
    const bucketName = document.getElementById("download-bucket").value;
    const fileName = document.getElementById("download-file").value;

    if (!bucketName || !fileName) {
        alert("Please enter both bucket name and file name.");
        return;
    }

    try {
        // Attempt to download the file using the API endpoint
        const response = await fetch(`${API_BASE_URL}/download/${bucketName}/${fileName}`);
        if (!response.ok) {
            // Retrieve error message as plain text if available
            const errorText = await response.text();
            throw new Error(errorText || "Unknown error during download.");
        }

        // Process the response as binary data
        const blob = await response.blob();

        // Create a temporary link element to trigger the file download
        const link = document.createElement("a");
        link.href = URL.createObjectURL(blob);
        link.download = fileName;
        link.click();
    } catch (error) {
        alert("Error downloading the file: " + error.message);
    }
});

// =================================================================
//            FUNCTION TO UPLOAD A FILE TO A BUCKET
// =================================================================
document.getElementById("upload-button").addEventListener("click", async () => {
    const bucketName = document.getElementById("upload-bucket").value;
    const fileInput = document.getElementById("upload-file");
    const file = fileInput.files[0];

    if (!bucketName || !file) {
        alert("Please provide a bucket name and select a file.");
        return;
    }

    try {
        // Create a FormData object to hold the file for the upload
        const formData = new FormData();
        formData.append("file", file);

        // Send a POST request to upload the file to the specified bucket
        const response = await fetch(`${API_BASE_URL}/upload/${bucketName}`, {
            method: "POST",
            body: formData
        });

        if (!response.ok) {
            // Retrieve error details from the response
            const errorData = await response.json();
            throw new Error(errorData.detail || "Unknown error during file upload.");
        }

        const data = await response.json();
        alert(data.message);
    } catch (error) {
        alert("Error uploading the file: " + error.message);
    }
});

// =================================================================
//            FUNCTION TO DELETE A FILE FROM A BUCKET
// =================================================================
document.getElementById("delete-button").addEventListener("click", async () => {
    const bucketName = document.getElementById("delete-bucket").value;
    const fileName = document.getElementById("delete-file").value;

    if (!bucketName || !fileName) {
        alert("Please enter a bucket and a file.");
        return;
    }

    try {
        // Send a DELETE request to remove the specified file from the bucket
        const response = await fetch(`${API_BASE_URL}/delete/${bucketName}/${fileName}`, {
            method: "DELETE",
        });
        if (!response.ok) {
            throw new Error("Error deleting the file.");
        }
        const data = await response.json();
        alert(data.message);
    } catch (error) {
        alert("Error deleting the file: " + error);
    }
});

// =================================================================
//            FUNCTION TO UPLOAD WEATHER DATA BASED ON COORDINATES
// =================================================================
document.getElementById("weather-button").addEventListener("click", async () => {
    const lat = document.getElementById("latitude").value;
    const lon = document.getElementById("longitude").value;

    if (!lat || !lon) {
        alert("Please enter both latitude and longitude.");
        return;
    }

    try {
        // Call the API endpoint to retrieve weather data for the given coordinates
        const response = await fetch(`${API_BASE_URL}/weather?lat=${lat}&lon=${lon}`);
        if (!response.ok) {
            // Retrieve error details if the request fails
            const errorData = await response.json();
            throw new Error(errorData.detail || "Unknown error during weather data upload.");
        }

        const data = await response.json();
        // Update the status message element with the response message
        const statusMessage = document.getElementById("weather-status-message");
        statusMessage.textContent = data.message;
    } catch (error) {
        alert("Error uploading weather data: " + error);
    }
});
