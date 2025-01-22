// Base URL de l'API FastAPI
const API_BASE_URL = "http://localhost:8000";

// ----------------------
//   LIST BUCKETS
// ----------------------
document.getElementById("list-buckets").addEventListener("click", async () => {
    try {
        const response = await fetch(`${API_BASE_URL}/buckets`);
        if (!response.ok) {
            throw new Error("Erreur lors de la récupération des buckets.");
        }
        const data = await response.json();
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

// ----------------------
//   LIST FILES
// ----------------------
document.getElementById("list-files").addEventListener("click", async () => {
    const bucketName = document.getElementById("bucket-name").value;
    if (!bucketName) {
        alert("Please enter a bucket.");
        return;
    }
    try {
        const response = await fetch(`${API_BASE_URL}/files/${bucketName}`);
        if (!response.ok) {
            throw new Error("Erreur lors de la récupération des fichiers.");
        }
        const data = await response.json();
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

// ----------------------
//   DOWNLOAD A FILE
// ----------------------
document.getElementById("download-button").addEventListener("click", async () => {
    const bucketName = document.getElementById("download-bucket").value;
    const fileName = document.getElementById("download-file").value;

    if (!bucketName || !fileName) {
        alert("Please enter both bucket name and file name.");
        return;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/download/${bucketName}/${fileName}`);
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || "Erreur inconnue lors du téléchargement.");
        }

        const data = await response.json();
        const blob = new Blob([data.content], { type: "text/plain" });
        const link = document.createElement("a");
        link.href = URL.createObjectURL(blob);
        link.download = fileName;
        link.click();
    } catch (error) {
        alert("Error downloading the file: " + error);
    }
});

// ----------------------
//   UPLOAD A FILE
// ----------------------
document.getElementById("upload-button").addEventListener("click", async () => {
    const bucketName = document.getElementById("upload-bucket").value;
    const fileInput = document.getElementById("upload-file");

    if (!bucketName || !fileInput.files.length) {
        alert("Please enter a bucket and select a file.");
        return;
    }

    const file = fileInput.files[0];
    const formData = new FormData();
    formData.append("file", file);

    try {
        const response = await fetch(`${API_BASE_URL}/upload/${bucketName}`, {
            method: "POST",
            body: formData,
        });
        const data = await response.json();
        alert(data.message);
    } catch (error) {
        alert("Error uploading the file: " + error);
    }
});

// ----------------------
//   DELETE A FILE
// ----------------------
document.getElementById("delete-button").addEventListener("click", async () => {
    const bucketName = document.getElementById("delete-bucket").value;
    const fileName = document.getElementById("delete-file").value;

    if (!bucketName || !fileName) {
        alert("Please enter a bucket and a file.");
        return;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/delete/${bucketName}/${fileName}`, {
            method: "DELETE",
        });
        if (!response.ok) {
            throw new Error("Erreur lors de la suppression du fichier.");
        }
        const data = await response.json();
        alert(data.message);
    } catch (error) {
        alert("Error deleting the file: " + error);
    }
});

// ----------------------
//   UPLOAD WEATHER DATA
// ----------------------
document.getElementById("weather-button").addEventListener("click", async () => {
    const lat = document.getElementById("latitude").value;
    const lon = document.getElementById("longitude").value;

    if (!lat || !lon) {
        alert("Please enter both latitude and longitude.");
        return;
    }

    try {
        const response = await fetch(`${API_BASE_URL}/weather?lat=${lat}&lon=${lon}`);
        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || "Erreur inconnue lors de l'envoi des données météo.");
        }

        const data = await response.json();
        const statusMessage = document.getElementById("weather-status-message");
        statusMessage.textContent = data.message;
    } catch (error) {
        alert("Error uploading weather data: " + error);
    }
});

